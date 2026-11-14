terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "aws" {
  region = "eu-west-3" # Paris

  default_tags {
    tags = {
      Project     = "Crypto-Whale-ETL"
      Environment = "Production"
      ManagedBy   = "Terraform"
    }
  }
}

# 1. Génération d'une clé SSH dédiée pour GitHub Actions
resource "tls_private_key" "deploy_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "etl_ec2_key" {
  key_name   = "crypto-etl-deploy-key"
  public_key = tls_private_key.deploy_key.public_key_openssh
}

# 2. Récupération de l'image officielle Ubuntu 22.04 LTS
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# 3. Security Group (Forteresse réseau)
resource "aws_security_group" "etl_sg" {
  name        = "crypto-etl-sg"
  description = "Regles strictes pour ETL (SSH, Metabase, Redpanda Console)"

  # Port 22 : SSH
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Port 3000 : Dashboard Metabase
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Port 8080 : Redpanda Console (Monitoring Kafka)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Trafic sortant illimité (API Binance, AWS S3, packages)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# 4. DATA LAKE S3 (Voie Froide)
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "datalake" {
  bucket        = "crypto-whale-datalake-${random_id.bucket_suffix.hex}"
  force_destroy = true # Permet à terraform destroy de vider le bucket sans erreur
}

# Blocage strict de tout accès public
resource "aws_s3_bucket_public_access_block" "datalake_private" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Règle anti-facturation : suppression automatique des fichiers après 30 jours
resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "purge-after-30-days"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }
  }
}

# 5. SÉCURITÉ IAM (Accès EC2 -> S3 sans clé secrète)
resource "aws_iam_role" "ec2_s3_role" {
  name = "ec2_datalake_writer_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "s3_write_policy" {
  name = "s3_datalake_write_policy"
  role = aws_iam_role.ec2_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "ec2_s3_profile" {
  name = "ec2_datalake_profile"
  role = aws_iam_role.ec2_s3_role.name
}

# 6. Provisionnement du Serveur EC2
resource "aws_instance" "etl_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = "t2.micro" # Éligible au Free Tier
  key_name      = aws_key_pair.etl_ec2_key.key_name

  vpc_security_group_ids = [aws_security_group.etl_sg.id]

  # Liaison du rôle IAM pour autoriser l'écriture directe sur S3
  iam_instance_profile = aws_iam_instance_profile.ec2_s3_profile.name

  # Empêche Terraform de détruire l'instance si Ubuntu sort une mise à jour d'AMI
  lifecycle {
    ignore_changes = [ami, user_data]
  }

  # Optimisation du disque dur pour stocker la base de données
  root_block_device {
    volume_size = 15 # Go
    volume_type = "gp3"
  }

  # Script de démarrage d'infrastructure (SWAP + Docker)
  user_data = <<-EOF
              #!/bin/bash
              
              # --- CRÉATION DU SWAP (2 Go) ---
              fallocate -l 2G /swapfile
              chmod 600 /swapfile
              mkswap /swapfile
              swapon /swapfile
              echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab

              # --- INSTALLATION DES DÉPENDANCES ---
              apt-get update -y
              apt-get install -y ca-certificates curl gnupg git

              # --- INSTALLATION OFFICIELLE DE DOCKER ---
              install -m 0755 -d /etc/apt/keyrings
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
              chmod a+r /etc/apt/keyrings/docker.gpg
              echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
              
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

              # --- PERMISSIONS ---
              usermod -aG docker ubuntu
              EOF
}

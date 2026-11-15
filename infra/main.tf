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

# --- Variables sensibles ---
variable "env_prod_content" {
  description = "Contenu du fichier .env pour la production"
  type        = string
  sensitive   = true
}

variable "github_token" {
  description = "Token GitHub pour le déploiement automatisé"
  type        = string
  sensitive   = true
}

# --- Paramètres SSM ---
resource "aws_ssm_parameter" "env_prod" {
  name        = "/usine-etl/env-prod"
  description = "Fichier .env de production pour l'ETL"
  type        = "SecureString"
  value       = var.env_prod_content
  overwrite   = true
}

resource "aws_ssm_parameter" "github_token" {
  name        = "/usine-etl/github-token"
  description = "Token GitHub pour cloner le repo"
  type        = "SecureString"
  value       = var.github_token
  overwrite   = true
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

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

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
  force_destroy = true 
}

resource "aws_s3_bucket_public_access_block" "datalake_private" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

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

# 5. SÉCURITÉ IAM (Accès EC2 -> S3 ET SSM)
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

# --- CORRIGÉ : Autorisation de lire les DEUX paramètres SSM ---
resource "aws_iam_role_policy" "ssm_read_policy" {
  name = "ssm_env_read_policy"
  role = aws_iam_role.ec2_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = [
        aws_ssm_parameter.env_prod.arn,
        aws_ssm_parameter.github_token.arn
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
  instance_type = "t2.micro" 
  key_name      = aws_key_pair.etl_ec2_key.key_name

  vpc_security_group_ids = [aws_security_group.etl_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_s3_profile.name

  lifecycle {
    ignore_changes = [ami, user_data]
  }

  root_block_device {
    volume_size = 15
    volume_type = "gp3"
  }

  user_data = <<-EOF
              #!/bin/bash
              
              # --- CRÉATION DU SWAP (2 Go) ---
              fallocate -l 2G /swapfile
              chmod 600 /swapfile
              mkswap /swapfile
              swapon /swapfile
              echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab

              # --- INSTALLATION DES DÉPENDANCES ET AWS CLI ---
              apt-get update -y
              apt-get install -y ca-certificates curl gnupg git awscli

              # --- INSTALLATION OFFICIELLE DE DOCKER ---
              install -m 0755 -d /etc/apt/keyrings
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
              chmod a+r /etc/apt/keyrings/docker.gpg
              echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
              
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

              usermod -aG docker ubuntu

              # --- RÉCUPÉRATION DES SECRETS ET DU CODE ---
              mkdir -p /home/ubuntu/Crypto_whale_detector
              
              # 1. Génération du fichier .env
              aws ssm get-parameter \
                --region eu-west-3 \
                --name "/usine-etl/env-prod" \
                --with-decryption \
                --query "Parameter.Value" \
                --output text > /home/ubuntu/Crypto_whale_detector/.env
              
              chown ubuntu:ubuntu /home/ubuntu/Crypto_whale_detector/.env
              chmod 600 /home/ubuntu/Crypto_whale_detector/.env

              # 2. Récupération du Token GitHub en mémoire
              GIT_TOKEN=$(aws ssm get-parameter --region eu-west-3 --name "/usine-etl/github-token" --with-decryption --query "Parameter.Value" --output text)

              # 3. Clonage silencieux (utilise le dossier /tmp pour ne pas écraser le .env)
              git clone https://$GIT_TOKEN@github.com/ahmed-beji/Crypto_whale_detector.git /tmp/repo
              cp -r /tmp/repo/* /home/ubuntu/Crypto_whale_detector/
              rm -rf /tmp/repo

              # 4. Lancement automatisé de l'infrastructure
              cd /home/ubuntu/Crypto_whale_detector
              sudo docker compose up -d
              EOF
}
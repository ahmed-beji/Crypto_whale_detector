output "EC2_HOST" {
  value       = aws_instance.etl_server.public_ip
  description = "L'adresse IP publique de votre instance EC2"
}

output "EC2_SSH_KEY" {
  value       = tls_private_key.deploy_key.private_key_pem
  description = "La clé privée SSH brute pour GitHub Actions"
  sensitive   = true
}

output "S3_BUCKET_NAME" {
  description = "Nom du bucket S3 pour la Voie Froide"
  value       = aws_s3_bucket.datalake.bucket
}
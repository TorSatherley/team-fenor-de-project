# AWS Project outputs
output "default_tags" {
  value = var.default_tags
}



output "aws_account_id" {
  value = local.aws_account_id
}

output "aws_region" {
  value = local.aws_region
}


# Bucket names 

output "s3_ingestion_bucket_name" {
  value = var.s3_ingestion_bucket
}

output "s3_processed_bucket_name" {
  value = var.s3_processed_bucket
}
# Output the ARN and ID for Data Ingestion Bucket
output "s3_ingestion_bucket_arn" {
  value = aws_s3_bucket.data_ingestion_bucket.arn
}

output "s3_ingestion_bucket_id" {
  value = aws_s3_bucket.data_ingestion_bucket.id
}

# Output the ARN and ID for Data Processed Bucket
output "s3_processed_bucket_arn" {
  value = aws_s3_bucket.data_processed_bucket.arn
}

output "s3_processed_bucket_id" {
  value = aws_s3_bucket.data_processed_bucket.id
}
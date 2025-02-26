variable "s3_ingestion_bucket_arn" {
  type = string
}

variable "s3_ingestion_bucket_id" {
  type = string
}

variable "s3_ingestion_bucket" {
  type = string
}

variable "s3_processed_bucket" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "aws_region" {
  type = string 
}

variable "lambda_extract_handler" {
  type = string
}

variable "lambda_transform_handler" {
  type = string
}

module "extract_module" {
  source         = "./extract_module"
  aws_account_id = local.aws_account_id
  aws_region     = local.aws_region
  default_tags   = var.default_tags
}

module "transform_module" {
  source                   = "./transform_module"
  aws_account_id           = local.aws_account_id
  aws_region               = local.aws_region
  s3_ingestion_bucket_arn  = "arn:aws:s3:::${var.s3_ingestion_bucket}"
  s3_ingestion_bucket_id   = var.s3_ingestion_bucket
  s3_ingestion_bucket      = var.s3_ingestion_bucket
  s3_processed_bucket      = var.s3_processed_bucket
  lambda_extract_handler   = var.lambda_extract_handler
  lambda_transform_handler = var.lambda_transform_handler
  lambda_load_handler = var.lambda_load_handler
}

module "load_module" {
  source = "./load_module"
  aws_account_id = local.aws_account_id
  aws_region     = local.aws_region
  s3_ingestion_bucket_arn = "arn:aws:s3:::${var.s3_ingestion_bucket}"
  s3_ingestion_bucket_id = "${var.s3_ingestion_bucket}"
  s3_ingestion_bucket = "${var.s3_ingestion_bucket}"
  s3_processed_bucket = "${var.s3_processed_bucket}"
  lambda_extract_handler = "${var.lambda_extract_handler}"
  lambda_transform_handler = "${var.lambda_transform_handler}"
  lambda_load_handler = "${var.lambda_load_handler}"
  }

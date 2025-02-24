######################
###  Vars // AWS 
######################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  aws_account_id = data.aws_caller_identity.current.account_id
  aws_region = data.aws_region.current.name
}


variable "env" {
  type    = string
  default = "Development"
}

variable "default_tags" {
  type = map(string)
  default = {
      Owner       = "Team Fenor"
      Project     = "Totesys DE Project"
      Environment = "Development"
  }
}

######################
### Vars // S3 buckets
######################

variable "s3_ingestion_bucket" {
  type = string
  default = "totesys-ingestion-zone-fenor"
}

variable "s3_processed_bucket" {
  type = string
  default = "totesys-processed-zone-fenor"
}

variable "s3_logging_bucket" {
  type = string
  default = "totesys-data-logging-fenor"
}

######################
### Vars // Function names 
######################

variable "lambda_extract_handler" {
  type = string 
  default = "lambda_extract_handler"
}

variable "lambda_transform_handler" {
  type = string 
  default = "lambda_transform_handler"
}

variable "lambda_load_handler" {
  type = string 
  default = "lambda_load_handler"
}


variable "totesys_etl_pipeline" {
  default = "totesys-etl-pipeline"
}


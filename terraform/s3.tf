# S3 Bucket - Extract Ingestion stage 
resource "aws_s3_bucket" "data_ingestion_bucket" {
  bucket        = var.s3_ingestion_bucket
  force_destroy = true
  tags = merge(
    var.default_tags,
    {
      Name = "S3_Ingestion_Bucket"
  })
}


# S3 Bucket - Transform Processed stage
resource "aws_s3_bucket" "data_processed_bucket" {
  bucket        = var.s3_processed_bucket
  force_destroy = true
  tags = merge(
    var.default_tags,
    {
      Name = "S3_Processed_Bucket"
  })
}


# S3 Bucket - Logging Ingestion and Processed buckets
resource "aws_s3_bucket" "data_logging_bucket" {
  bucket        = var.s3_logging_bucket
  force_destroy = true
  tags = merge(
    var.default_tags,
    {
      Name = "S3_Logging_Bucket"
  })
}


# S3 - Versioning / Immutable Data

# resource "aws_s3_bucket_versioning" "s3_versioning_ingestion" {
#   bucket = aws_s3_bucket.data_ingestion_bucket.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }

# resource "aws_s3_bucket_object_lock_configuration" "s3_object_lock_ingestion" {
#   bucket = aws_s3_bucket.data_ingestion_bucket.id

#   rule {
#     default_retention {
#       mode = "COMPLIANCE"
#       days = 1
#     }
#   }
# }

# resource "aws_s3_bucket_versioning" "s3_versioning_processed" {
#   bucket = aws_s3_bucket.data_processed_bucket.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }

# resource "aws_s3_bucket_object_lock_configuration" "s3_versioning_processed" {
#   bucket = aws_s3_bucket.data_processed_bucket.id

#   rule {
#     default_retention {
#       mode = "COMPLIANCE"
#       days = 1
#     }
#   }
# }


# S3 Logging Bucket 

resource "aws_s3_bucket_logging" "s3_logging_ingestion" {
  bucket        = aws_s3_bucket.data_ingestion_bucket.id
  target_bucket = aws_s3_bucket.data_logging_bucket.id
  target_prefix = "ingestion-logs/"
  
}

resource "aws_s3_bucket_logging" "s3_logging_processed" {
  bucket        = aws_s3_bucket.data_processed_bucket.id
  target_bucket = aws_s3_bucket.data_logging_bucket.id
  target_prefix = "processed-logs/"
}


resource "aws_s3_bucket_policy" "logging_bucket_policy" {
  bucket = aws_s3_bucket.data_logging_bucket.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "logging.s3.amazonaws.com"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.data_logging_bucket.arn}/*"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}
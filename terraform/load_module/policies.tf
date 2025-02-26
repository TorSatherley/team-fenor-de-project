resource "aws_iam_policy" "lambda_load_execute" {
  name        = "LambdaLoadExecutePolicy"
  description = "Allows Lambda to invoke other Lambda functions"

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:*"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_load_invoke" {
  name        = "LambdaLoadInvokePolicy"
  description = "Allows Lambda to invoke other Lambda functions"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action   = "lambda:InvokeFunction"
        Effect   = "Allow"
        Resource = [
          "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:${var.lambda_load_handler}"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_load_cloudwatch_logs" {
  name        = "LambdaLoadCloudWatchLogsPolicy"
  description = "Allows Lambda to create and write logs"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "logs:CreateLogGroup"
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.lambda_load_handler}:*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_load_s3_full_access" {
  name        = "LambdaLoadS3FullAccessPolicy"
  description = "Full access to S3"

  policy = jsonencode({
    Version   = "2012-10-17"
    Id        = "default"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:${var.lambda_load_handler}"
        Condition = {
          StringEquals = {
            "AWS:SourceAccount" = "${var.aws_account_id}"
          }
          ArnLike = {
            "AWS:SourceArn" = "arn:aws:s3:::${var.s3_ingestion_bucket}"
          }
        }
      }
    ]
  })
}
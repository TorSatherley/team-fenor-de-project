# Lambda Extract - Policies

resource "aws_iam_policy" "lambda_extract_invoke" {
  name        = "LambdaInvokePolicy"
  description = "Allows Lambda to invoke other Lambda functions"

  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "default"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:${var.lambda_extract_handler}"
        Condition = {
          ArnLike = {
            "AWS:SourceArn" = "arn:aws:events:${var.aws_region}:${var.aws_account_id}:rule/${var.lambda_transform_handler}"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_extract_cloudwatch_logs" {
  name        = "CloudWatchLogsPolicy"
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
        Resource = [
          "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.lambda_extract_handler}:*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_extract_s3_full_access" {
  name        = "S3FullAccessPolicy"
  description = "Allows full access to S3"

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:*",
          "s3-object-lambda:*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_extrat_secrets_manager_access" {
  name        = "LambdaExtractSecretsManagerAccessPolicy"
  description = "Allow Lambda Extract to access AWS Secrets Manager"

  # Define the policy document
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "*"
      }
    ]
  })
}



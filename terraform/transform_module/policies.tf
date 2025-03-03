resource "aws_iam_policy" "lambda_transform_execute" {
  name        = "LambdaTransformExecutePolicy"
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

resource "aws_iam_policy" "lambda_transform_invoke" {
  name        = "LambdaTransformInvokePolicy"
  description = "Allows Lambda to invoke other Lambda functions"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action   = "lambda:InvokeFunction"
        Effect   = "Allow"
        Resource = [
          "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:${var.lambda_transform_handler}",
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_transform_cloudwatch_logs" {
  name        = "LambdaTransformCloudWatchLogsPolicy"
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
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.lambda_transform_handler}:*"
      }
    ]
  })
}

# resource "aws_iam_policy" "lambda_transform_s3_full_access" {
#   name        = "LambdaTransformS3FullAccessPolicy"
#   description = "Full access to S3"

#   policy = jsonencode({
#     Version   = "2012-10-17"
#     Id        = "default"
#     Statement = [
#       {
#         Effect   = "Allow"
#         Action   = "lambda:InvokeFunction"
#         Resource = "arn:aws:lambda:${var.aws_region}:${var.aws_account_id}:function:${var.lambda_transform_handler}"
#         Condition = {
#           StringEquals = {
#             "AWS:SourceAccount" = "${var.aws_account_id}"
#           }
#           ArnLike = {
#             "AWS:SourceArn" = "arn:aws:s3:::${var.s3_ingestion_bucket}"
#           }
#         }
#       }
#     ]
#   })
# }

resource "aws_iam_policy" "lambda_transform_s3_full_access" {
  name        = "LambdaTransformS3FullAccessPolicy"
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
# Lambda Transform - IAM role

resource "aws_iam_role" "lambda_transform_exec" {
  name               = "lambda_transform_role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Action   = "sts:AssumeRole"
        Effect   = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Attachments to IAM role

resource "aws_iam_role_policy_attachment" "lambda_transform_attach_s3" {
  role       = aws_iam_role.lambda_transform_exec.name
  policy_arn = aws_iam_policy.lambda_transform_s3_full_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_transform_attach_logs" {
  role       = aws_iam_role.lambda_transform_exec.name
  policy_arn = aws_iam_policy.lambda_transform_cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "lambda_transform_invoke" {
  role       = aws_iam_role.lambda_transform_exec.name
  policy_arn = aws_iam_policy.lambda_transform_invoke.arn
}

resource "aws_iam_role_policy_attachment" "lambda_transform_execute" {
  role       = aws_iam_role.lambda_transform_exec.name
  policy_arn = aws_iam_policy.lambda_transform_execute.arn
}
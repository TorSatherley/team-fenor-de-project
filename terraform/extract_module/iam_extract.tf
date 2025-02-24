####################
### ROLE // Lambda
####################

resource "aws_iam_role" "lambda_extract_iam_role" {
  name = "lambda_extract_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

####################
### POLICY ATTACHMENTS // 
###   - s3_full_access
###   - cloudwatch logs
###   - lambda invoke
####################

resource "aws_iam_role_policy_attachment" "lambda_extract_attach_s3" {
  role       = aws_iam_role.lambda_extract_iam_role.name
  policy_arn = aws_iam_policy.lambda_extract_s3_full_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_extract_attach_logs" {
  role       = aws_iam_role.lambda_extract_iam_role.name
  policy_arn = aws_iam_policy.lambda_extract_cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "lambda_extract_invoke" {
  role       = aws_iam_role.lambda_extract_iam_role.name
  policy_arn = aws_iam_policy.lambda_extract_invoke.arn
}

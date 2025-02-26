# Lambda Load - IAM role

resource "aws_iam_role" "lambda_load_exec" {
  name               = "lambda_load_role"
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

# Attatch Policties

resource "aws_iam_role_policy_attachment" "lambda_load_attach_s3" {
  role       = aws_iam_role.lambda_load_exec.name
  policy_arn = aws_iam_policy.lambda_load_s3_full_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_load_attach_logs" {
  role       = aws_iam_role.lambda_load_exec.name
  policy_arn = aws_iam_policy.lambda_load_cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "lambda_load_invoke" {
  role       = aws_iam_role.lambda_load_exec.name
  policy_arn = aws_iam_policy.lambda_load_invoke.arn
}

resource "aws_iam_role_policy_attachment" "lambda_load_execute" {
  role       = aws_iam_role.lambda_load_exec.name
  policy_arn = aws_iam_policy.lambda_load_execute.arn
}
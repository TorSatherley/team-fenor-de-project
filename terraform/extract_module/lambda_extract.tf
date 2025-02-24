#########################################################
############# Lambda Function - Extract
########################################################
# resource "aws_lambda_function" "lambda_extract_handler" {
#   filename         = data.archive_file.lambda_extract_package.output_path
#   function_name    = "${var.lambda_extract_handler}"
#   runtime          = "python3.13"
#   role             = aws_iam_role.lambda_extract_iam_role.arn
#   handler          = "lambda_extract.lambda_handler"
#   source_code_hash = data.archive_file.lambda_extract_package.output_base64sha256
#   depends_on = [
#     aws_iam_policy.lambda_extract_cloudwatch_logs,
#     aws_iam_role_policy_attachment.lambda_extract_attach_logs
#   ]
#   tags = merge(
#     var.default_tags,
#     {
#       Name = "Lambda: Extract from RDS"
#     })
# }

# data "archive_file" "lambda_extract_package" {
#   type        = "zip"
#   source_dir  = "${path.module}/src"
#   output_path = "${path.module}/src/lambda_extract.zip"
# }

# Add layers here if required. 





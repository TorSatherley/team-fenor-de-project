# Lambda Function - Extract
resource "aws_lambda_function" "lambda_extract_handler" {
  filename         = data.archive_file.lambda_extract_package.output_path
  function_name    = "${var.lambda_extract_handler}"
  runtime          = "python3.13"
  role             = aws_iam_role.lambda_extract_iam_role.arn
  handler          = "lambda_extract.lambda_handler"
  timeout          =  200
  layers            = [ aws_lambda_layer_version.lambda_extract_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:1" ]
  source_code_hash = data.archive_file.lambda_extract_package.output_base64sha256
  depends_on = [
    aws_iam_policy.lambda_extract_cloudwatch_logs,
    aws_iam_role_policy_attachment.lambda_extract_attach_logs
  ]
  tags = merge(
    var.default_tags,
    {
      Name = "Lambda: Extract from RDS"
    })
}

data "archive_file" "lambda_extract_package" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/src/lambda_extract.zip"
}

# Layers 
# Added AWS SDK Pandas arn to lambda function  

resource "null_resource" "pip_install" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/requirements.txt"))}"
  }
  provisioner "local-exec" {
    command = "python3 -m pip install -r ${path.module}/requirements.txt -t ${path.module}/${var.lambda_extract_handler}_layer/python"
  }
}

# Lambda Layer Archive
data "archive_file" "lambda_extract_layer" {
  type        = "zip"
  source_dir  = "${path.module}/${var.lambda_extract_handler}_layer"
  output_path = "${path.module}/src/layer.zip"
  depends_on  = [null_resource.pip_install]
}

resource "aws_lambda_layer_version" "lambda_extract_layer" {
  layer_name          = "${var.lambda_extract_handler}_layer"
  filename            = data.archive_file.lambda_extract_layer.output_path
  source_code_hash    = data.archive_file.lambda_extract_layer.output_base64sha256
  compatible_runtimes = ["python3.13", "python3.12", "python3.11", "python3.10", "python3.9"]
}





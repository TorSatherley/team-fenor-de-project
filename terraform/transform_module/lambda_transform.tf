# Lambda Function - Transform
resource "aws_lambda_function" "lambda_transform_handler" {
  function_name    = "${var.lambda_transform_handler}"
  role             = aws_iam_role.lambda_transform_exec.arn
  runtime          = "python3.13"
  handler          = "lambda_transform.lambda_handler"
  timeout         = 200
  layers            = [aws_lambda_layer_version.lambda_transform_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:1"]
  filename         = data.archive_file.lambda_transform_package.output_path
  source_code_hash = data.archive_file.lambda_transform_package.output_base64sha256
}

data "archive_file" "lambda_transform_package" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/lambda_transform.zip"
}


# Layers 
# Added AWS SDK Pandas arn to lambda function  

resource "null_resource" "pip_install" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/requirements.txt"))}"
  }
  provisioner "local-exec" {
    command = "python3 -m pip install -r ${path.module}/requirements.txt -t ${path.module}/${var.lambda_transform_handler}_layer/python"
  }
}

# Lambda Layer Archive
data "archive_file" "lambda_transform_layer" {
  type        = "zip"
  source_dir  = "${path.module}/${var.lambda_transform_handler}_layer"
  output_path = "${path.module}/layer_transform.zip"
  depends_on  = [null_resource.pip_install]
}

resource "aws_lambda_layer_version" "lambda_transform_layer" {
  layer_name          = "${var.lambda_transform_handler}_layer"
  filename            = data.archive_file.lambda_transform_layer.output_path
  source_code_hash    = data.archive_file.lambda_transform_layer.output_base64sha256
  compatible_runtimes = ["python3.13", "python3.12", "python3.11", "python3.10", "python3.9"]
}
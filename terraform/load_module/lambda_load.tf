# Lambda Load - Function
resource "aws_lambda_function" "lambda_load_handler" {
  function_name    = "lambda_load_handler"
  role             = aws_iam_role.lambda_load_exec.arn
  runtime          = "python3.13"
  handler          = "src.lambda_load.lambda_handler"
  timeout          = 600
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:1",aws_lambda_layer_version.psycopg2_layer.arn, aws_lambda_layer_version.lambda_load_layer.arn]
  filename         = data.archive_file.lambda_load_package.output_path
  source_code_hash = data.archive_file.lambda_load_package.output_base64sha256
  environment {
    variables = {
      SECRET_NAME = "data-warehouse-credentials"
      BUCKET_NAME = "totesys-processed-zone-fenor"
    }
  }
}

# data "archive_file" "lambda_load_package" {
#   type        = "zip"
#   source_dir  = "${path.module}/../../src"
#   output_path = "${path.module}/lambda_load.zip"
# }

data "archive_file" "lambda_load_package" {
  type        = "zip"
  output_path = "${path.module}/lambda_load.zip"
  source {
    content = file("${path.module}/../../src/lambda_load.py")
    filename = "src/lambda_load.py"
  }
  source {
    content = file("${path.module}/../../src/utils.py")
    filename = "src/utils.py"
  }
    source {
    content = file("${path.module}/../../src/lambda_transform_utils.py")
    filename = "src/lambda_transform_utils.py"
  }
}


# Layers 
# Added AWS SDK Pandas arn to lambda function  

resource "null_resource" "pip_install" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/requirements.txt"))}"
  }
  provisioner "local-exec" {
    command = "python3 -m pip install -r ${path.module}/requirements.txt -t ${path.module}/${var.lambda_load_handler}_layer/python"
  }
}

# Lambda Layer Archive
data "archive_file" "lambda_load_layer" {
  type        = "zip"
  source_dir  = "${path.module}/${var.lambda_load_handler}_layer"
  output_path = "${path.module}/layer_load.zip"
  depends_on  = [null_resource.pip_install]
}

resource "aws_lambda_layer_version" "lambda_load_layer" {
  layer_name          = "${var.lambda_load_handler}_layer"
  filename            = data.archive_file.lambda_load_layer.output_path
  source_code_hash    = data.archive_file.lambda_load_layer.output_base64sha256
  compatible_runtimes = ["python3.13", "python3.12", "python3.11", "python3.10", "python3.9"]
}

resource "aws_lambda_layer_version" "psycopg2_layer" {
  layer_name = "psycopg2_layer"
  filename = "${path.module}/psycopg2.zip"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = ["python3.13"]
}
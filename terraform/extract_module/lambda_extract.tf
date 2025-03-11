# Lambda Function - Extract
resource "aws_lambda_function" "lambda_extract_handler" {
  filename         = data.archive_file.lambda_extract_package.output_path
  function_name    = "${var.lambda_extract_handler}"
  runtime          = "python3.13"
  role             = aws_iam_role.lambda_extract_iam_role.arn
  handler          = "src.lambda_extract.lambda_handler"
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
  environment {
    variables = {
      SECRET_NAME = "totesys-db-credentials"
      BUCKET_NAME = "totesys-ingestion-zone-fenor"
    }
  } 
}


# data "archive_file" "lambda_extract_package" {
#   type        = "zip"
#   source_dir  = "${path.module}/../../"
#   #source_file = "${path.module}/../../src/"
#   output_path = "${path.module}/lambda_extract.zip"
#   #excludes    = ["Makefile", "data", "requirements.txt", "terraform", "venv.nosync", "README.md", "db", "test"]

# }

data "archive_file" "lambda_extract_package" {
  type        = "zip"
  output_path = "${path.module}/lambda_extract.zip"
  source {
    content = file("${path.module}/../../src/lambda_extract.py")
    filename = "src/lambda_extract.py"
  }
  source {
    content = file("${path.module}/../../src/utils.py")
    filename = "src/utils.py"
  }
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
  output_path = "${path.module}/layer_extract.zip"
  depends_on  = [null_resource.pip_install]
}

resource "aws_lambda_layer_version" "lambda_extract_layer" {
  layer_name          = "${var.lambda_extract_handler}_layer"
  filename            = data.archive_file.lambda_extract_layer.output_path
  source_code_hash    = data.archive_file.lambda_extract_layer.output_base64sha256
  compatible_runtimes = ["python3.13", "python3.12", "python3.11", "python3.10", "python3.9"]
}





resource "aws_sfn_state_machine" "totesys_etl_pipeline" {
  name       = "${var.totesys_etl_pipeline}"
  role_arn   = aws_iam_role.lambda_exec_role.arn
  type       = "STANDARD"
  definition = jsonencode({
    "StartAt" = "Extract",
    "States" = {
      "Extract" = {
        "Type" = "Task",
        "Resource" = "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_extract_handler}",
        "Next" = "Transform"
      },
      "Transform" = {
        "Type" = "Task",
        "Resource" = "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_transform_handler}",
        "Next" = "Load"
      },
      "Load" = {
        "Type" = "Task",
        "Resource" = "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_load_handler}",
        "End" = true
      }
    }
  })
}

resource "aws_iam_role" "lambda_exec_role" {
  name = "step_function_lambda_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "step_function_lambda_policy" {
  name        = "StepFunctionLambdaInvoke"
  description = "Allows Step Functions to invoke Lambda functions"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = "lambda:InvokeFunction",
        Resource = [
            "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_extract_handler}",
            "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_transform_handler}",
            "arn:aws:lambda:${local.aws_region}:${local.aws_account_id}:function:${var.lambda_load_handler}",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_sfn_policy" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.step_function_lambda_policy.arn
}
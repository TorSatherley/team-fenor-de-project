# resource "aws_cloudwatch_event_rule" "lambda_extract_trigger" {
#   name                = "lambda-extract-trigger"
#   description         = "Triggers EventBridge event to extract"
#   schedule_expression = "cron(0/20 * * * ? *)"
# }

# Event Target
# resource "aws_cloudwatch_event_target" "lambda_extract_event_target" {
#   rule      = aws_cloudwatch_event_rule.lambda_extract_trigger.name
#   target_id = "lambda_extract"
#   arn       = aws_lambda_function.lambda_extract_handler.arn
# }

# Permission to Invoke Lambda
# resource "aws_lambda_permission" "eventbridge_allow_cloudwatch" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.lambda_extract_handler.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.lambda_extract_trigger.arn
# }
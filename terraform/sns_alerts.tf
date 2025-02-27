###########################################################################
#####   SNS alerts - Lambda Errors
###########################################################################

resource "aws_sns_topic" "alarms_for_errors" {
  name = "alarms_for_errors"
}

resource "aws_sns_topic_subscription" "lambda_extract_sns_email" {
  topic_arn = aws_sns_topic.alarms_for_errors.arn
  protocol  = "email"
  endpoint  = "brendanc8450@gmail.com"
  depends_on = [
    aws_sns_topic.alarms_for_errors
  ]
}

resource "aws_sns_topic_policy" "step_function_sns_policy" {
  arn    = aws_sns_topic.alarms_for_errors.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    effect  = "Allow"
    actions = ["SNS:Publish"]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com", "events.amazonaws.com"]
    }
    resources = [aws_sns_topic.alarms_for_errors.arn]
  }
}



###########################################################################
#####   SNS alert - Metrics (adapt when familiar with data streams)
###########################################################################

resource "aws_cloudwatch_metric_alarm" "alarm_lambda_extract_errors" {
  alarm_name          = "alarm_lambda_extract_errors"
  alarm_description   = "Alarm for error logs in cloudwatch"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  actions_enabled     = true
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alarms_for_errors.arn]
  dimensions = {
    FunctionName = "lambda_extract_handler"
  }
}

resource "aws_cloudwatch_metric_alarm" "alarm_lambda_transform_errors" {
  alarm_name          = "alarm_lambda_transform_errors"
  alarm_description   = "Alarm for Lambda Transform errors"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  actions_enabled     = true
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alarms_for_errors.arn]
  dimensions = {
    FunctionName = "lambda_extract_handler"
  }
}

resource "aws_cloudwatch_metric_alarm" "alarm_lambda_load_errors" {
  alarm_name          = "alarm_lambda_load_errors"
  alarm_description   = "Alarm for Lambda Load errors"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  actions_enabled     = true
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alarms_for_errors.arn]
  dimensions = {
    FunctionName = "lambda_extract_handler"
  }
}
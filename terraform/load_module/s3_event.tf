resource "aws_lambda_permission" "lambda_load_s3_notification_allow" {
  statement_id  = "LambdaLoadAllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_load_handler.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.s3_processed_bucket}"
  source_account = "${var.aws_account_id}"
}

resource "aws_s3_bucket_notification" "lambda_load_bucket_notification" {
  bucket = "${var.s3_processed_bucket}"

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_load_handler.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "log/"
    filter_suffix       = ".log"
  }

  depends_on = [
    aws_lambda_permission.lambda_load_s3_notification_allow,
    aws_iam_role_policy_attachment.lambda_load_invoke
  ]
}


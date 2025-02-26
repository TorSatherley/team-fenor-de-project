resource "aws_lambda_permission" "lambda_transform_s3_notification_allow" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_transform_handler.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::totesys-ingestion-zone-fenor"
  source_account = "${var.aws_account_id}"

}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = "${var.s3_ingestion_bucket}"

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_transform_handler.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix = "log/"
    filter_suffix       = ".log"
  }

  depends_on = [
    aws_lambda_permission.lambda_transform_s3_notification_allow,
    aws_iam_role_policy_attachment.lambda_transform_invoke
  ]
}


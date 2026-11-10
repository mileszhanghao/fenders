output "bucket" {
  description = "Set as the repository variable FENDERS_BUCKET."
  value       = aws_s3_bucket.data.id
}

output "role_arn" {
  description = "Set as the repository variable AWS_ROLE_ARN."
  value       = aws_iam_role.publisher.arn
}

output "freshness_function" {
  description = "Run it by hand: aws lambda invoke --function-name <this> out.json"
  value       = aws_lambda_function.freshness.function_name
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

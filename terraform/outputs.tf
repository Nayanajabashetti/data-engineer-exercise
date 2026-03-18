output "glue_job_name" {
  description = "Name of the deployed Glue job."
  value       = aws_glue_job.analyzer.name
}

output "s3_bucket_name" {
  description = "Name of the S3 data bucket."
  value       = aws_s3_bucket.data.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data bucket."
  value       = aws_s3_bucket.data.arn
}

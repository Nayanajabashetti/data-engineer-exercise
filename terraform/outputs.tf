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

output "lambda_function_name" {
  description = "Name of the deployed Lambda function (if enabled)."
  value       = var.enable_lambda ? aws_lambda_function.analyzer[0].function_name : null
}

output "ssm_parameter_name" {
  description = "Name of the SSM SecureString parameter used by Lambda."
  value       = var.ssm_parameter_name
}

output "s3_landing_prefix" {
  description = "Landing (raw) layer prefix under the data bucket."
  value       = var.input_prefix
}

output "s3_staging_prefix" {
  description = "Staging (silver) layer prefix for optional partitioned Parquet hits."
  value       = var.staging_prefix
}

output "s3_curated_prefix" {
  description = "Curated (gold) layer prefix for aggregated outputs."
  value       = var.output_prefix
}

output "partition_interval_minutes" {
  description = "Hive minute= bucket interval (Lambda env + Glue job arg)."
  value       = var.partition_interval_minutes
}

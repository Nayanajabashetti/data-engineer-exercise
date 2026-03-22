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

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI URL (if enable_mwaa)."
  value       = var.enable_mwaa ? module.mwaa[0].mwaa_webserver_url : null
}

output "mwaa_environment_name" {
  description = "MWAA environment name (if enable_mwaa)."
  value       = var.enable_mwaa ? var.mwaa_environment_name : null
}

output "mwaa_dag_s3_uri" {
  description = "S3 URI prefix where DAGs should be synced (if enable_mwaa)."
  value       = var.enable_mwaa ? "s3://${aws_s3_bucket.data.id}/${trimsuffix(var.mwaa_dag_s3_path, "/")}/" : null
}

output "mwaa_requirements_s3_uri" {
  description = "S3 URI of MWAA requirements.txt (Terraform-managed; if enable_mwaa)."
  value       = var.enable_mwaa ? "s3://${aws_s3_bucket.data.id}/${replace(trimsuffix(var.mwaa_dag_s3_path, "/"), "/dags", "")}/requirements.txt" : null
}

output "mwaa_security_group_id" {
  description = "MWAA managed security group id (for RDS/VPC peering rules; if enable_mwaa)."
  value       = var.enable_mwaa ? module.mwaa[0].mwaa_security_group_id : null
}

output "mwaa_data_bucket_ssm_parameter_name" {
  description = "SSM parameter name holding the data bucket (Airflow DAG reads this when Variable search_keyword_bucket is unset)."
  value       = var.enable_mwaa ? var.mwaa_data_bucket_ssm_parameter_name : null
}

output "mwaa_network_vpc_id" {
  description = "VPC created for MWAA when create_mwaa_network=true."
  value       = var.enable_mwaa && var.create_mwaa_network ? aws_vpc.mwaa[0].id : null
}

output "mwaa_network_private_subnet_ids" {
  description = "Private subnet IDs created for MWAA when create_mwaa_network=true."
  value       = var.enable_mwaa && var.create_mwaa_network ? [aws_subnet.mwaa_private[0].id, aws_subnet.mwaa_private[1].id] : null
}

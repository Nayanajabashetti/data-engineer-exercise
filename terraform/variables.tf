variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-west-2"
}

variable "bucket_name" {
  description = "Name of the S3 bucket for input and output data."
  type        = string
}

variable "input_prefix" {
  description = "Landing layer (raw / bronze): hit-level Parquet (default) or legacy TSV. Lambda trigger prefix; Glue reads from here."
  type        = string
  default     = "landing/"
}

variable "staging_prefix" {
  description = "Staging layer (silver): optional Glue output — partitioned Parquet hits (dt/hour/minute). Set empty \"\" to skip staging writes."
  type        = string
  default     = "staging/search_hits/"
}

variable "partition_interval_minutes" {
  description = "Hive minute= bucket size (1–60). Must match partition_time / Lambda PARTITION_INTERVAL_MINUTES / Glue --partition_interval_minutes."
  type        = number
  default     = 15
}

variable "output_prefix" {
  description = "Curated layer (gold / cleansed): aggregated keyword performance Parquet; Lambda writes here too."
  type        = string
  default     = "curated/search_keyword/"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X = 4 vCPU/16GB, G.2X = 8 vCPU/32GB)."
  type        = string
  default     = "G.1X"
}

variable "glue_worker_count" {
  description = "Number of Glue workers. Scale up for larger files."
  type        = number
  default     = 2
}

variable "glue_enable_large_job_optimizations" {
  description = "When true, Glue job sets Spark AQE/shuffle tuning (see src/glue_job.py)."
  type        = bool
  default     = false
}

variable "glue_shuffle_partitions" {
  description = "Spark spark.sql.shuffle.partitions when large-job optimizations are enabled."
  type        = number
  default     = 200
}

variable "glue_curated_output_partitions" {
  description = "Number of Parquet files for curated output (1 = single file, typical for small aggregates; 8–32 for very large groupBy results)."
  type        = number
  default     = 1
}

variable "glue_visitor_repartition_partitions" {
  description = "If > 0, repartition by visitor_id before window functions (0 = disabled). Try 2–4x total executor cores for huge inputs."
  type        = number
  default     = 0
}

variable "glue_staging_repartition_partitions" {
  description = "If > 0 and staging prefix is set, repartition before writing silver Parquet (0 = Spark default)."
  type        = number
  default     = 0
}

variable "glue_s3_recursive_list" {
  description = "Glue S3 CSV connection recurse=true (default). Set false if TSVs are only direct children of resolved_input (no nested hour=/minute= folders) to trim S3 listing."
  type        = bool
  default     = true
}

variable "glue_landing_format" {
  description = "Glue landing layer format: parquet (default) or tsv (legacy tab-separated with header)."
  type        = string
  default     = "parquet"
}

variable "enable_lambda" {
  description = "Whether to deploy the S3-triggered Lambda path (recommended for small files)."
  type        = bool
  default     = true
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 300
}

variable "ssm_parameter_name" {
  description = "Name of the SecureString parameter stored in SSM Parameter Store."
  type        = string
  default     = "/search-keyword-performance/api-key"
}

variable "enable_db_sinks" {
  description = "Whether Lambda/Glue should also write aggregates to database sinks."
  type        = bool
  default     = false
}

variable "db_host" {
  description = "PostgreSQL host endpoint used by Lambda/Glue DB sink."
  type        = string
  default     = ""
}

variable "db_port" {
  description = "PostgreSQL port used by Lambda/Glue DB sink."
  type        = number
  default     = 5432
}

variable "db_name" {
  description = "PostgreSQL database name for DB sink."
  type        = string
  default     = ""
}

variable "db_secret_arn" {
  description = "Secrets Manager ARN for RDS/Postgres credentials. When set, Glue/Lambda IAM includes GetSecretValue on this ARN (required if Airflow or job args use sync_db_sinks with this secret)."
  type        = string
  default     = ""
}

variable "db_fact_table" {
  description = "Target PostgreSQL fact table for keyword performance."
  type        = string
  default     = "fact_keyword_performance"
}

variable "db_ai_table" {
  description = "Target PostgreSQL table for keyword AI insights."
  type        = string
  default     = "ai_keyword_insights"
}

variable "lambda_subnet_ids" {
  description = "VPC subnet IDs for Lambda (required with security groups to reach private RDS)."
  type        = list(string)
  default     = []
}

variable "lambda_security_group_ids" {
  description = "Security group IDs for Lambda ENIs (must allow egress to RDS SG on 5432)."
  type        = list(string)
  default     = []
}

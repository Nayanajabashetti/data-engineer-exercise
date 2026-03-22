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
  description = "Secrets Manager ARN containing PostgreSQL username/password."
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

# --- Optional: Amazon MWAA (Airflow in AWS) ---

variable "enable_mwaa" {
  description = "Deploy Amazon MWAA using the data bucket for DAGs (prefix mwaa_dag_s3_path). Requires VPC + 2+ private subnets."
  type        = bool
  default     = false
}

variable "mwaa_environment_name" {
  description = "MWAA environment name (must be unique per account/region)."
  type        = string
  default     = "search-keyword-airflow"
}

variable "create_mwaa_network" {
  description = "When enable_mwaa=true, create a dedicated VPC (10.42.0.0/16 by default) with 2 private subnets in 2 AZs, 1 public subnet, IGW + single NAT. Ignores mwaa_vpc_id/mwaa_private_subnet_ids. Recommended instead of default-VPC public subnets."
  type        = bool
  default     = false
}

variable "mwaa_vpc_cidr" {
  description = "CIDR for the optional MWAA VPC (when create_mwaa_network=true)."
  type        = string
  default     = "10.42.0.0/16"
}

variable "mwaa_vpc_id" {
  description = "VPC ID where MWAA runs when create_mwaa_network=false (private subnets must have NAT or endpoints for egress)."
  type        = string
  default     = ""
}

variable "mwaa_private_subnet_ids" {
  description = "At least two private subnet IDs in different AZs when create_mwaa_network=false."
  type        = list(string)
  default     = []
}

variable "mwaa_dag_s3_path" {
  description = "S3 prefix for DAGs inside the data bucket (no s3:// prefix). Default matches scripts/sync_airflow_dags_to_s3.sh."
  type        = string
  default     = "airflow/dags"
}

variable "mwaa_data_bucket_ssm_parameter_name" {
  description = "SSM Parameter Store name (String) whose value is the data bucket name. Airflow DAGs read this when Variable search_keyword_bucket is unset. Must match DATA_BUCKET_SSM_PARAM in airflow/dags/search_keyword_pipeline_dag.py if you change it."
  type        = string
  default     = "/search-keyword-performance/airflow/data_bucket_name"
}

variable "mwaa_airflow_version" {
  description = "Apache Airflow version on MWAA (must be a version supported in your region; see AWS docs)."
  type        = string
  default     = "2.10.3"
}

variable "mwaa_environment_class" {
  description = "MWAA worker environment size (e.g. mw1.small). See AWS pricing."
  type        = string
  default     = "mw1.small"
}

variable "mwaa_webserver_access_mode" {
  description = "PUBLIC_ONLY (UI over internet) or PRIVATE_ONLY (VPN / VPC only)."
  type        = string
  default     = "PUBLIC_ONLY"
}

variable "mwaa_ui_allowed_cidrs" {
  description = "CIDR blocks allowed inbound to HTTPS (443) on the MWAA security group for UI access (ignored if empty; module may still create self-referencing rules)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "mwaa_min_workers" {
  description = "Minimum Airflow workers for MWAA."
  type        = number
  default     = 1
}

variable "mwaa_max_workers" {
  description = "Maximum Airflow workers for MWAA (1–25)."
  type        = number
  default     = 10
}

variable "mwaa_airflow_configuration_options" {
  description = "Extra Airflow config key/values merged into MWAA (see AWS MWAA docs)."
  type        = map(string)
  default     = {}
}

variable "mwaa_tags" {
  description = "Extra tags for MWAA module resources."
  type        = map(string)
  default     = {}
}

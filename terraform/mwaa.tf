# Optional: Amazon MWAA (Managed Workflows for Apache Airflow) using the same S3 data bucket
# for DAGs under prefix `airflow/dags/` (see scripts/sync_airflow_dags_to_s3.sh).
#
# Requires: VPC + at least two private subnets with NAT (or equivalent) for worker egress.

check "mwaa_networking" {
  assert {
    condition = !var.enable_mwaa || var.create_mwaa_network || (
      var.mwaa_vpc_id != "" && length(var.mwaa_private_subnet_ids) >= 2
    )
    error_message = "When enable_mwaa=true, either set create_mwaa_network=true (Terraform builds a VPC + 2 private subnets + NAT) or set mwaa_vpc_id and at least two mwaa_private_subnet_ids (different AZs)."
  }
}

# Lets the DAG resolve the data bucket without Airflow Variables (see search_keyword_pipeline_dag._resolve_data_bucket).
resource "aws_ssm_parameter" "mwaa_airflow_data_bucket" {
  count = var.enable_mwaa ? 1 : 0

  name  = var.mwaa_data_bucket_ssm_parameter_name
  type  = "String"
  value = aws_s3_bucket.data.id
}

locals {
  # e.g. airflow/dags -> airflow/requirements.txt (same folder family as DAG prefix)
  mwaa_requirements_s3_key = "${replace(trimsuffix(var.mwaa_dag_s3_path, "/"), "/dags", "")}/requirements.txt"

  mwaa_policy_statements = concat(
    [
      {
        Sid    = "GlueJobSearchKeyword"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:GetJob",
          "glue:BatchStopJobRun",
        ]
        Resource = aws_glue_job.analyzer.arn
      },
      {
        Sid    = "S3DataBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:PutObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
    ],
    var.enable_lambda ? [
      {
        Sid    = "LambdaInvokeSearchKeyword"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        # String ARN so Terraform does not reference aws_lambda_function.analyzer[0] when count=0.
        Resource = "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:search-keyword-performance"
      }
    ] : [],
    var.db_secret_arn != "" ? [
      {
        Sid      = "SecretsForDbVerify"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.db_secret_arn
      }
    ] : [],
    var.enable_mwaa ? [
      {
        Sid      = "SSMAirflowDataBucketName"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = aws_ssm_parameter.mwaa_airflow_data_bucket[0].arn
      }
    ] : []
  )
}

resource "aws_iam_policy" "mwaa_airflow_extra" {
  count = var.enable_mwaa ? 1 : 0

  name        = "${var.bucket_name}-mwaa-airflow-extra"
  description = "Glue, S3 data bucket, optional Lambda invoke, and optional Secrets Manager for search-keyword Airflow DAGs on MWAA."
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = local.mwaa_policy_statements
  })
}

# MWAA installs these packages on workers/schedulers at environment update time (see AWS MWAA requirements.txt).
resource "aws_s3_object" "mwaa_requirements" {
  count = var.enable_mwaa ? 1 : 0

  bucket = aws_s3_bucket.data.id
  key    = local.mwaa_requirements_s3_key
  source = "${path.module}/../airflow/requirements.txt"
  etag   = filemd5("${path.module}/../airflow/requirements.txt")
}

module "mwaa" {
  source  = "aws-ia/mwaa/aws"
  version = "0.0.6"

  count = var.enable_mwaa ? 1 : 0

  name              = var.mwaa_environment_name
  airflow_version   = var.mwaa_airflow_version
  environment_class = var.mwaa_environment_class

  vpc_id             = local.mwaa_vpc_id_effective
  private_subnet_ids = local.mwaa_private_subnet_ids_effective

  # Reuse the Terraform-managed data bucket; sync DAGs to s3://<bucket>/airflow/dags/
  create_s3_bucket  = false
  source_bucket_arn = aws_s3_bucket.data.arn
  dag_s3_path       = trimsuffix(var.mwaa_dag_s3_path, "/")

  requirements_s3_path           = local.mwaa_requirements_s3_key
  requirements_s3_object_version = aws_s3_object.mwaa_requirements[0].version_id

  webserver_access_mode = var.mwaa_webserver_access_mode
  source_cidr           = var.mwaa_ui_allowed_cidrs

  min_workers = var.mwaa_min_workers
  max_workers = var.mwaa_max_workers

  iam_role_additional_policies = {
    "search-keyword-pipeline" = aws_iam_policy.mwaa_airflow_extra[0].arn
  }

  airflow_configuration_options = merge(
    {
      "core.load_default_connections" = "false"
      "core.load_examples"            = "false"
      "logging.logging_level"         = "INFO"
    },
    var.mwaa_airflow_configuration_options,
  )

  tags = merge(
    var.mwaa_tags,
    { "Project" = "search-keyword-performance" },
  )
}

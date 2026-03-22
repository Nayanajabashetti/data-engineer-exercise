terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.5"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Bundle Lambda handler + pg8000 (and deps) into one zip. Requires `python3` + `pip` on the machine running terraform apply.
resource "null_resource" "lambda_bundle" {
  count = var.enable_lambda ? 1 : 0

  triggers = {
    py_sources = sha256(join("", [for f in sort(fileset("${path.module}/../src", "*.py")) : filesha256("${path.module}/../src/${f}")]))
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -e
      BUILD="${path.module}/lambda_build"
      rm -rf "$BUILD"
      mkdir -p "$BUILD"
      cp -R "${path.module}/../src/"* "$BUILD/"
      # Linux wheels for Lambda (x86_64). On Apple Silicon, plain pip can install wrong arch.
      if command -v docker >/dev/null 2>&1; then
        # Lambda image ENTRYPOINT expects a handler; override so we only run pip.
        docker run --platform linux/amd64 --rm -v "$BUILD:/var/task" \
          --entrypoint /bin/bash public.ecr.aws/lambda/python:3.12 \
          -c "pip install pg8000==1.31.5 'pyarrow>=17.0,<19' -t /var/task --no-cache-dir"
      else
        python3 -m pip install pg8000==1.31.5 "pyarrow>=17.0,<19" -t "$BUILD" --no-cache-dir
      fi
    EOT
  }
}

data "archive_file" "lambda_zip" {
  count = var.enable_lambda ? 1 : 0

  depends_on = [null_resource.lambda_bundle]

  type        = "zip"
  source_dir  = "${path.module}/lambda_build"
  output_path = "${path.module}/lambda_payload.zip"
  excludes    = ["__pycache__/*", "*.pyc", ".DS_Store"]
}

# --- S3 buckets ---

resource "aws_s3_bucket" "data" {
  bucket = var.bucket_name

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "data_versioning" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Spark/Hadoop may create S3 placeholder keys like "staging_$folder$" before writing
# "staging/search_hits/dt=.../...". IAM must allow PutObject on that key; a rule for only
# "staging/search_hits/*" does not match "staging_$folder$".
locals {
  glue_staging_top_prefix_arn = (
    trim(var.staging_prefix, "/") != ""
    ? "${aws_s3_bucket.data.arn}/${element(split("/", trim(var.staging_prefix, "/")), 0)}*"
    : null
  )
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data.id
  key    = "scripts/glue_job.py"
  source = "${path.module}/../src/glue_job.py"
  etag   = filemd5("${path.module}/../src/glue_job.py")
}

# --- Glue job ---

resource "aws_glue_job" "analyzer" {
  name         = "search-keyword-performance"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  max_retries  = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data.id}/scripts/glue_job.py"
    python_version  = "3"
  }

  default_arguments = merge(
    {
      "--input_path"                       = "s3://${aws_s3_bucket.data.id}/${var.input_prefix}"
      "--output_path"                      = "s3://${aws_s3_bucket.data.id}/${var.output_prefix}"
      "--partition_interval_minutes"       = tostring(var.partition_interval_minutes)
      "--job-language"                     = "python"
      "--enable-continuous-cloudwatch-log" = "true"
      "--additional-python-modules"        = "pg8000==1.31.5"
      "--sync_db_sinks"                    = var.enable_db_sinks ? "true" : "false"
      "--db_host"                          = var.db_host
      "--db_port"                          = tostring(var.db_port)
      "--db_name"                          = var.db_name
      "--db_secret_arn"                    = var.db_secret_arn
      "--db_fact_table"                    = var.db_fact_table
      "--db_ai_table"                      = var.db_ai_table
    },
    var.staging_prefix != "" ? { "--partitioned_hits_path" = "s3://${aws_s3_bucket.data.id}/${var.staging_prefix}" } : {},
    {
      "--enable_large_job_optimizations" = var.glue_enable_large_job_optimizations ? "true" : "false"
      "--shuffle_partitions"             = tostring(var.glue_shuffle_partitions)
      "--curated_output_partitions"      = tostring(var.glue_curated_output_partitions)
      "--visitor_repartition_partitions" = tostring(var.glue_visitor_repartition_partitions)
      "--staging_repartition_partitions" = tostring(var.glue_staging_repartition_partitions)
      "--s3_recursive_list"              = var.glue_s3_recursive_list ? "true" : "false"
      "--landing_format"                 = var.glue_landing_format
    }
  )

  number_of_workers = var.glue_worker_count
  worker_type       = var.glue_worker_type
}

# --- Optional: S3 event -> EventBridge -> Glue trigger ---

resource "aws_glue_trigger" "on_demand" {
  name = "search-keyword-on-demand"
  type = "ON_DEMAND"

  actions {
    job_name = aws_glue_job.analyzer.name
  }
}

# --- IAM role for Glue ---

resource "aws_iam_role" "glue_role" {
  name = "search-keyword-performance-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-data-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = concat(
          [
            "${aws_s3_bucket.data.arn}/${var.output_prefix}*",
            "${aws_s3_bucket.data.arn}/output*",
            "${aws_s3_bucket.data.arn}/curated*",
          ],
          trim(var.staging_prefix, "/") != "" ? [
            "${aws_s3_bucket.data.arn}/${var.staging_prefix}*",
            local.glue_staging_top_prefix_arn,
          ] : []
        )
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_secrets_read" {
  count = var.enable_db_sinks && var.db_secret_arn != "" ? 1 : 0
  name  = "glue-secrets-db-credentials"
  role  = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.db_secret_arn
      }
    ]
  })
}

# --- Lambda function (optional) ---

resource "aws_iam_role" "lambda_role" {
  count = var.enable_lambda ? 1 : 0
  name  = "search-keyword-performance-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  count = var.enable_lambda ? 1 : 0
  name  = "search-keyword-performance-lambda-policy"
  role  = aws_iam_role.lambda_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/search-keyword-performance:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
        ]
        Resource = "${aws_s3_bucket.data.arn}/${var.input_prefix}*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.data.arn}/${var.output_prefix}*"
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
        ]
        Resource = "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${var.ssm_parameter_name}"
      },
    ]
  })
}

resource "aws_iam_role_policy" "lambda_secrets_read" {
  count = var.enable_lambda && var.enable_db_sinks && var.db_secret_arn != "" ? 1 : 0
  name  = "lambda-secrets-db-credentials"
  role  = aws_iam_role.lambda_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.db_secret_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_access" {
  count = var.enable_lambda && length(var.lambda_subnet_ids) > 0 && length(var.lambda_security_group_ids) > 0 ? 1 : 0

  role       = aws_iam_role.lambda_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "analyzer" {
  count = var.enable_lambda ? 1 : 0

  function_name = "search-keyword-performance"
  role          = aws_iam_role.lambda_role[0].arn
  runtime       = "python3.12"
  handler       = "lambda_handler.handler"

  filename         = data.archive_file.lambda_zip[0].output_path
  source_code_hash = data.archive_file.lambda_zip[0].output_base64sha256

  timeout     = var.lambda_timeout_seconds
  memory_size = var.lambda_memory_size

  dynamic "vpc_config" {
    for_each = length(var.lambda_subnet_ids) > 0 && length(var.lambda_security_group_ids) > 0 ? [1] : []
    content {
      subnet_ids         = var.lambda_subnet_ids
      security_group_ids = var.lambda_security_group_ids
    }
  }

  environment {
    variables = {
      OUTPUT_PREFIX              = var.output_prefix
      PARTITION_INTERVAL_MINUTES = tostring(var.partition_interval_minutes)
      API_KEY_PARAM              = var.ssm_parameter_name
      SYNC_DB_SINKS              = var.enable_db_sinks ? "true" : "false"
      DB_HOST                    = var.db_host
      DB_PORT                    = tostring(var.db_port)
      DB_NAME                    = var.db_name
      DB_SECRET_ARN              = var.db_secret_arn
      DB_FACT_TABLE              = var.db_fact_table
      DB_AI_TABLE                = var.db_ai_table
    }
  }
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  count = var.enable_lambda ? 1 : 0

  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analyzer[0].function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

resource "aws_s3_bucket_notification" "input_trigger" {
  count  = var.enable_lambda ? 1 : 0
  bucket = aws_s3_bucket.data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.analyzer[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.input_prefix
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

data "aws_caller_identity" "current" {}

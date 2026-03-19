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
  }
}

provider "aws" {
  region = var.aws_region
}

# Needed to package Lambda source into a zip.
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/lambda_payload.zip"
  excludes    = ["__pycache__/*", "*.pyc", ".DS_Store"]
}

# --- S3 buckets ---

resource "aws_s3_bucket" "data" {
  bucket = var.bucket_name

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = true
  }
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

  default_arguments = {
    "--input_path"                       = "s3://${aws_s3_bucket.data.id}/${var.input_prefix}"
    "--output_path"                      = "s3://${aws_s3_bucket.data.id}/${var.output_prefix}"
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
  }

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
        Resource = [
          # Normal outputs (e.g., output/2026-03-19_SearchKeywordPerformance/...)
          "${aws_s3_bucket.data.arn}/${var.output_prefix}*",
          # Some Spark/S3 integrations may attempt to write a legacy folder-marker
          # object like output_$folder$ at the bucket root.
          "${aws_s3_bucket.data.arn}/output*",
        ]
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

resource "aws_lambda_function" "analyzer" {
  count = var.enable_lambda ? 1 : 0

  function_name = "search-keyword-performance"
  role          = aws_iam_role.lambda_role[0].arn
  runtime       = "python3.12"
  handler       = "lambda_handler.handler"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  timeout     = var.lambda_timeout_seconds
  memory_size = var.lambda_memory_size

  environment {
    variables = {
      OUTPUT_PREFIX = var.output_prefix
      API_KEY_PARAM = var.ssm_parameter_name
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

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 buckets ---

resource "aws_s3_bucket" "data" {
  bucket = var.bucket_name
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
    "--input_path"                   = "s3://${aws_s3_bucket.data.id}/${var.input_prefix}"
    "--output_path"                  = "s3://${aws_s3_bucket.data.id}/${var.output_prefix}"
    "--job-language"                 = "python"
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

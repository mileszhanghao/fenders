# AWS for Fenders: a private S3 bucket for the published data, a role GitHub Actions can
# assume without stored keys (OIDC), and a daily Lambda that checks the pipeline is alive.
#
#   cd infra
#   terraform init
#   terraform apply -var 'alert_email=you@example.com'
#
# Then add the `role_arn` and `bucket` outputs as repository variables AWS_ROLE_ARN and
# FENDERS_BUCKET (Settings > Secrets and variables > Actions > Variables).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "fenders", managed_by = "terraform" }
  }
}

data "aws_caller_identity" "me" {}

locals {
  bucket = "${var.name}-${data.aws_caller_identity.me.account_id}"
}

# --- Storage --------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket = local.bucket
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Dated snapshots are history, not hot data: move them to cheaper storage, then expire them.
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "age-out-snapshots"
    status = "Enabled"
    filter {
      prefix = "marts/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = var.snapshot_retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# --- GitHub Actions: short-lived credentials via OIDC, no stored keys -----------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  count           = var.create_github_oidc_provider ? 1 : 0
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc_provider ? 0 : 1
  url   = "https://token.actions.githubusercontent.com"
}

locals {
  github_oidc_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github[0].arn
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # Only the main branch of this one repository may publish.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "publisher" {
  name               = "${var.name}-github-publisher"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

# Least privilege: write and list this one bucket, nothing else.
data "aws_iam_policy_document" "publish" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }
}

resource "aws_iam_role_policy" "publish" {
  name   = "publish-to-bucket"
  role   = aws_iam_role.publisher.id
  policy = data.aws_iam_policy_document.publish.json
}

# --- Alerts ---------------------------------------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# --- Freshness Lambda -----------------------------------------------------------------------------------------------

data "archive_file" "freshness" {
  type        = "zip"
  source_file = "${path.module}/../aws/freshness/handler.py"
  output_path = "${path.module}/build/freshness.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "freshness" {
  name               = "${var.name}-freshness"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "freshness_logs" {
  role       = aws_iam_role.freshness.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "freshness" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/_manifest.json"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Fenders"]
    }
  }
}

resource "aws_iam_role_policy" "freshness" {
  name   = "freshness-check"
  role   = aws_iam_role.freshness.id
  policy = data.aws_iam_policy_document.freshness.json
}

resource "aws_lambda_function" "freshness" {
  function_name    = "${var.name}-freshness"
  role             = aws_iam_role.freshness.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.freshness.output_path
  source_code_hash = data.archive_file.freshness.output_base64sha256
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      BUCKET               = aws_s3_bucket.data.id
      TOPIC_ARN            = aws_sns_topic.alerts.arn
      MAX_AGE_HOURS        = tostring(var.max_age_hours)
      MAX_WEATHER_LAG_DAYS = tostring(var.max_weather_lag_days)
    }
  }
}

resource "aws_cloudwatch_log_group" "freshness" {
  name              = "/aws/lambda/${aws_lambda_function.freshness.function_name}"
  retention_in_days = 14
}

# Daily at 18:00 UTC (10:00/11:00 Seattle), a few hours after the pipeline's 14:17 UTC run.
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.name}-freshness-daily"
  schedule_expression = "cron(0 18 * * ? *)"
}

resource "aws_cloudwatch_event_target" "freshness" {
  rule = aws_cloudwatch_event_rule.daily.name
  arn  = aws_lambda_function.freshness.arn
}

resource "aws_lambda_permission" "events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.freshness.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}

# A second line of defence: if the Lambda itself stops reporting, alarm anyway.
resource "aws_cloudwatch_metric_alarm" "not_healthy" {
  alarm_name          = "${var.name}-not-healthy"
  namespace           = "Fenders"
  metric_name         = "Healthy"
  statistic           = "Minimum"
  period              = 86400
  evaluation_periods  = 1
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}


# S3
resource "aws_s3_bucket" "nse_platform" {
  bucket = "nse-platform"
}
resource "aws_s3_bucket_server_side_encryption_configuration" "nse_platform" {
  bucket = aws_s3_bucket.nse_platform.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "nse_platform" {
  bucket = aws_s3_bucket.nse_platform.id

  rule {
    id     = "expire-old-wheels"
    status = "Enabled"

    filter {
      prefix = "artifacts/"
    }

    expiration {
      days = 30
    }
  }
}


# IAM - for glue
resource "aws_iam_role" "nse_glue_job_role" {
  name = "nse-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  description = "Allows Glue to call AWS services on your behalf. "
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.nse_glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_console_full_access" {
  role       = aws_iam_role.nse_glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# IAM - for S3
resource "aws_iam_policy" "nse_platform_s3_access" {
  name = "nse-platform-s3-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "NseObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::nse-platform/*"
      },
      {
        Sid    = "NseBucketList"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = "arn:aws:s3:::nse-platform"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.nse_glue_job_role.name
  policy_arn = aws_iam_policy.nse_platform_s3_access.arn
}

# IAM - for lambda
resource "aws_iam_role" "nse_ingest_role" {
  name = "nse-ingest-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  description = "Allows Lambda functions to call AWS services on your behalf."
}

resource "aws_iam_role_policy_attachment" "ingest_s3_access" {
  role       = aws_iam_role.nse_ingest_role.name
  policy_arn = aws_iam_policy.nse_platform_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "ingest_s3_object_lambda" {
  role       = aws_iam_role.nse_ingest_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonS3ObjectLambdaExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "ingest_lambda_basic_execution" {
  role       = aws_iam_role.nse_ingest_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


# MWAA
resource "aws_iam_role" "mwaa_nse_environment_role" {
  name = "MWAA-NSE_Environment_role"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "airflow.amazonaws.com",
            "airflow-env.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "nse_mwaa_orchestration_access" {
  name = "nse-mwaa-orchestration-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TriggerAndPollGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJob",
          "glue:GetJobs",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "arn:aws:glue:us-east-1:978942695091:job/*"
      },
      {
        Sid      = "InvokeIngestLambdas"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = [
          "arn:aws:lambda:us-east-1:978942695091:function:nse-equity-ingest-Function",
          "arn:aws:lambda:us-east-1:978942695091:function:nse-index-ingest-Function"
        ]
      },
      {
        Sid      = "PassGlueJobRole"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:aws:iam::978942695091:role/nse-glue-job-role"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_orchestration_access" {
  role       = aws_iam_role.mwaa_nse_environment_role.name
  policy_arn = aws_iam_policy.nse_mwaa_orchestration_access.arn
}

resource "aws_iam_policy" "mwaa_execution_policy" {
  name = "MWAA-Execution-Policy-2c5095f5-b617-4693-bd6a-dbe59a0999c4"
  path = "/service-role/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "airflow:PublishMetrics"
        Resource = "arn:aws:airflow:us-east-1:978942695091:environment/NSEairflowEnvironment"
      },
      {
        Effect = "Deny"
        Action = "s3:ListAllMyBuckets"
        Resource = [
          "arn:aws:s3:::nse-platform",
          "arn:aws:s3:::nse-platform/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:GetBucket*",
          "s3:List*"
        ]
        Resource = [
          "arn:aws:s3:::nse-platform",
          "arn:aws:s3:::nse-platform/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults"
        ]
        Resource = [
          "arn:aws:logs:us-east-1:978942695091:log-group:airflow-NSEairflowEnvironment-*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage"
        ]
        Resource = "arn:aws:sqs:us-east-1:*:airflow-celery-*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey*",
          "kms:Encrypt"
        ]
        NotResource = "arn:aws:kms:*:978942695091:key/*"
        Condition = {
          StringLike = {
            "kms:ViaService" = ["sqs.us-east-1.amazonaws.com"]
          }
        }
      },
      {
        Sid    = "ReadGlueJobLogs"
        Effect = "Allow"
        Action = [
          "logs:GetLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:us-east-1:978942695091:log-group:/aws-glue/jobs/output:*",
          "arn:aws:logs:us-east-1:978942695091:log-group:/aws-glue/jobs/error:*",
          "arn:aws:logs:us-east-1:978942695091:log-group:/aws-glue/jobs/logs-v2:*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_execution_policy" {
  role       = aws_iam_role.mwaa_nse_environment_role.name
  policy_arn = aws_iam_policy.mwaa_execution_policy.arn
}

#Github actions
resource "aws_iam_role" "nse_github_actions_deploy" {
  name = "nse-github-actions-deploy"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = "arn:aws:iam::978942695091:oidc-provider/token.actions.githubusercontent.com"
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:branesh2k/nse_data_platform:ref:refs/heads/main"
          }
        }
      }
    ]
  })

  description = "Assumed by GitHub Actions from main branch to deploy artifacts"
}

resource "aws_iam_role_policy" "nse_actions_publish_artifacts" {
  name = "nse-actions-publish-artifacts"
  role = aws_iam_role.nse_github_actions_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishWheelToS3"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "arn:aws:s3:::nse-platform/artifacts/*"
      },
      {
        Sid      = "PublishDagsToS3"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "arn:aws:s3:::nse-platform/dags/*"
      },
      {
        Sid      = "ECRLoginTokenAccountWide"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "PushImageToNseIngestRepo"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage"
        ]
        Resource = "arn:aws:ecr:us-east-1:978942695091:repository/nse-ingest"
      },
      {
        Sid      = "UpdateLambdaFunctions"
        Effect   = "Allow"
        Action   = "lambda:UpdateFunctionCode"
        Resource = [
          "arn:aws:lambda:us-east-1:978942695091:function:nse-equity-ingest-Function",
          "arn:aws:lambda:us-east-1:978942695091:function:nse-index-ingest-Function"
        ]
      },
            {
        Sid      = "CreateLambdaFunctions"
        Effect   = "Allow"
        Action   = ["lambda:GetFunction", "lambda:CreateFunction"]
        Resource = [
          "arn:aws:lambda:us-east-1:978942695091:function:nse-equity-ingest-Function",
          "arn:aws:lambda:us-east-1:978942695091:function:nse-index-ingest-Function"
        ]
      },
      {
        Sid      = "PassIngestRoleToLambda"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:aws:iam::978942695091:role/nse-ingest-role"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "lambda.amazonaws.com"
          }
        }
      },
            {
        Sid      = "PassGlueRoleForJobUpdate"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:aws:iam::978942695091:role/nse-glue-job-role"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      },
      {
        Sid      = "UpdateGlueJobs"
        Effect   = "Allow"
        Action   = ["glue:GetJob", "glue:UpdateJob"]
        Resource = "arn:aws:glue:us-east-1:978942695091:job/*"
      },
      
    ]
  })
}


#ECR
resource "aws_ecr_repository" "nse_ingest" {
  name                 = "nse-ingest"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

resource "aws_ecr_repository_policy" "nse_ingest_lambda_access" {
  repository = aws_ecr_repository.nse_ingest.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowLambdaPull"
        Effect    = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
      }
    ]
  })
}

resource "aws_ecr_lifecycle_policy" "nse_ingest" {
  repository = aws_ecr_repository.nse_ingest.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

#GLUE
resource "aws_glue_job" "bronze_equity" {
  name     = "bronzeEquity"
  role_arn = aws_iam_role.nse_glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://nse-platform/artifacts/glue_entry/bronze_equity.py"
    python_version  = "3"
  }
  

  default_arguments = {
    "--data_root"                       = "s3://nse-platform"
    "--enable-glue-datacatalog"         = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--TempDir"                         = "s3://aws-glue-assets-978942695091-us-east-1/temporary/"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://aws-glue-assets-978942695091-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--additional-python-modules"       = "s3://nse-platform/artifacts/nse_platform-0.1.1-py3-none-any.whl"
    "--enable-observability-metrics"    = "true"
    "--conf"                            = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--run-date"                        = "2026-06-01"
    "--storage_backend"                 = "s3"
  }
  lifecycle {
    ignore_changes = [default_arguments["--additional-python-modules"]]
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries       = 0
  timeout           = 60
  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.1"
  execution_class   = "STANDARD"
}

resource "aws_glue_job" "bronze_index" {
  name     = "BronzeIndex"
  role_arn = aws_iam_role.nse_glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://nse-platform/artifacts/glue_entry/bronze_index.py"
    python_version  = "3"
  }

  default_arguments = {
    "--data_root"                       = "s3://nse-platform"
    "--enable-glue-datacatalog"         = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--datalake-formats"                = "delta"
    "--TempDir"                         = "s3://aws-glue-assets-978942695091-us-east-1/temporary/"
    "--run_date"                        = "2026-06-01"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://aws-glue-assets-978942695091-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--additional-python-modules"       = "s3://nse-platform/artifacts/nse_platform-0.1.1-py3-none-any.whl"
    "--enable-observability-metrics"    = "true"
    "--conf"                            = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--storage_backend"                 = "s3"
  }
  lifecycle {
    ignore_changes = [default_arguments["--additional-python-modules"]]
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries       = 0
  timeout           = 60
  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.1"
  execution_class   = "STANDARD"
}

resource "aws_glue_job" "silver_equity" {
  name     = "silverEquity"
  role_arn = aws_iam_role.nse_glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://nse-platform/artifacts/glue_entry/silver_equity.py"
    python_version  = "3"
  }

  default_arguments = {
    "--data_root"                       = "s3://nse-platform"
    "--contracts_root"                   = "s3://nse-platform/contracts"
    "--enable-glue-datacatalog"         = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--datalake-formats"                = "delta"
    "--TempDir"                         = "s3://aws-glue-assets-978942695091-us-east-1/temporary/"
    "--run_date"                        = "2026-06-01"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://aws-glue-assets-978942695091-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--additional-python-modules"       = "s3://nse-platform/artifacts/nse_platform-0.1.1-py3-none-any.whl"
    "--enable-observability-metrics"    = "true"
    "--conf"                            = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--storage_backend"                 = "s3"
  }
  lifecycle {
    ignore_changes = [default_arguments["--additional-python-modules"]]
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries       = 0
  timeout           = 60
  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.1"
  execution_class   = "STANDARD"
}

resource "aws_glue_job" "silver_index" {
  name     = "silverIndex"
  role_arn = aws_iam_role.nse_glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://nse-platform/artifacts/glue_entry/silver_index.py"
    python_version  = "3"
  }

  default_arguments = {
    "--data_root"                       = "s3://nse-platform"
    "--contracts_root"                   = "s3://nse-platform/contracts"
    "--enable-glue-datacatalog"         = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--datalake-formats"                = "delta"
    "--TempDir"                         = "s3://aws-glue-assets-978942695091-us-east-1/temporary/"
    "--run_date"                        = "2026-06-01"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://aws-glue-assets-978942695091-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--additional-python-modules"       = "s3://nse-platform/artifacts/nse_platform-0.1.1-py3-none-any.whl"
    "--enable-observability-metrics"    = "true"
    "--conf"                            = "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--storage_backend"                 = "s3"
  }
  lifecycle {
    ignore_changes = [default_arguments["--additional-python-modules"]]
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries       = 0
  timeout           = 60
  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.1"
  execution_class   = "STANDARD"
}

resource "aws_glue_job" "gold_job" {
  name     = "goldJob"
  role_arn = aws_iam_role.nse_glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://nse-platform/artifacts/glue_entry/gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--data_root"                       = "s3://nse-platform"
    "--enable-glue-datacatalog"         = "true"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--datalake-formats"                = "delta"
    "--TempDir"                         = "s3://aws-glue-assets-978942695091-us-east-1/temporary/"
    "--run_date"                        = "2026-06-01"
    "--enable-metrics"                  = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://aws-glue-assets-978942695091-us-east-1/sparkHistoryLogs/"
    "--enable-job-insights"             = "true"
    "--additional-python-modules"       = "s3://nse-platform/artifacts/nse_platform-0.1.1-py3-none-any.whl"
    "--enable-observability-metrics"    = "true"
    "--conf"                            = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                    = "python"
    "--storage_backend"                 = "s3"
  }
  lifecycle {
    ignore_changes = [default_arguments["--additional-python-modules"]]
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries       = 0
  timeout           = 60
  worker_type       = "G.1X"
  number_of_workers = 2
  glue_version      = "5.1"
  execution_class   = "STANDARD"
}


# =========================================================
# MWAA NETWORKING
# =========================================================

resource "aws_vpc" "mwaa" {
  cidr_block           = "10.192.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "MWAA-VPC"
  }
}

resource "aws_internet_gateway" "mwaa" {
  vpc_id = aws_vpc.mwaa.id

  tags = {
    Name = "MWAA-VPC-IGW"
  }
}

resource "aws_subnet" "public_1" {
  vpc_id                  = aws_vpc.mwaa.id
  cidr_block              = "10.192.10.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "MWAA-VPC-PublicSubnet1"
  }
}

resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.mwaa.id
  cidr_block              = "10.192.11.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true

  tags = {
    Name = "MWAA-VPC-PublicSubnet2"
  }
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.mwaa.id
  cidr_block        = "10.192.20.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "MWAA-VPC-PrivateSubnet1"
  }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.mwaa.id
  cidr_block        = "10.192.21.0/24"
  availability_zone = "us-east-1b"

  tags = {
    Name = "MWAA-VPC-PrivateSubnet2"
  }
}

resource "aws_eip" "nat_1" {
  domain = "vpc"
  tags = {
    Name = "MWAA-VPC-EIP1"
  }
}

resource "aws_eip" "nat_2" {
  domain = "vpc"
  tags = {
    Name = "MWAA-VPC-EIP2"
  }
}

resource "aws_nat_gateway" "nat_1" {
  allocation_id = aws_eip.nat_1.id
  subnet_id     = aws_subnet.public_1.id

  tags = {
    Name = "MWAA-VPC-NAT1"
  }

  depends_on = [aws_internet_gateway.mwaa]
}

resource "aws_nat_gateway" "nat_2" {
  allocation_id = aws_eip.nat_2.id
  subnet_id     = aws_subnet.public_2.id

  tags = {
    Name = "MWAA-VPC-NAT2"
  }

  depends_on = [aws_internet_gateway.mwaa]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.mwaa.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.mwaa.id
  }

  tags = {
    Name = "MWAA-VPC-PublicRouteTable"
  }
}

resource "aws_route_table_association" "public_1" {
  subnet_id      = aws_subnet.public_1.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_2" {
  subnet_id      = aws_subnet.public_2.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private_1" {
  vpc_id = aws_vpc.mwaa.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_1.id
  }

  tags = {
    Name = "MWAA-VPC-PrivateRouteTable1"
  }
}

resource "aws_route_table_association" "private_1" {
  subnet_id      = aws_subnet.private_1.id
  route_table_id = aws_route_table.private_1.id
}

resource "aws_route_table" "private_2" {
  vpc_id = aws_vpc.mwaa.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_2.id
  }

  tags = {
    Name = "MWAA-VPC-PrivateRouteTable2"
  }
}

resource "aws_route_table_association" "private_2" {
  subnet_id      = aws_subnet.private_2.id
  route_table_id = aws_route_table.private_2.id
}

resource "aws_security_group" "mwaa" {
  name        = "nse-mwaa-sg"
  description = "MWAA environment security group"
  vpc_id      = aws_vpc.mwaa.id

  tags = {
    Name = "NSEairflowEnvironment-sg"
  }
}

resource "aws_security_group_rule" "mwaa_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.mwaa.id
  source_security_group_id = aws_security_group.mwaa.id
}

resource "aws_security_group_rule" "mwaa_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.mwaa.id
  cidr_blocks       = ["0.0.0.0/0"]
}

# =========================================================
# MWAA ENVIRONMENT
# =========================================================

resource "aws_mwaa_environment" "nse_airflow" {
  name              = "NSEairflowEnvironment"
  airflow_version   = "3.2.1"
  environment_class = "mw1.micro"

  source_bucket_arn = aws_s3_bucket.nse_platform.arn
  dag_s3_path       = "dags/"

  execution_role_arn = aws_iam_role.mwaa_nse_environment_role.arn

  network_configuration {
    subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_group_ids = [aws_security_group.mwaa.id]
  }

  webserver_access_mode = "PUBLIC_ONLY"

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }
}
import aws_cdk as cdk
from aws_cdk import aws_s3
from constructs import Construct


class Bucket(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        account: str,
        region: str,
        env_name_string: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # provision an s3 bucket
        access_logs_bucket = aws_s3.Bucket(
            self,
            "AccessLogsBucket",
            bucket_name="access-logs-{}-{}-{}".format(account, region, env_name_string),
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
        )
        self.bucket = aws_s3.Bucket(
            self,
            "code-review",
            bucket_name="code-review-result-{}-{}-{}".format(
                account, region, env_name_string
            ),
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="logs",
        )

        self.lambda_log_bucket = aws_s3.Bucket(
            self,
            "lambda-log",
            bucket_name="lambda-log-{}-{}-{}".format(account, region, env_name_string),
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="lambda-log",
        )

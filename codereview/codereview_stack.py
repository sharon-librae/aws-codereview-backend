try:
    from aws_cdk import core as cdk
except ImportError:
    import aws_cdk as cdk

from aws_cdk import (
    # Duration,
    Aspects,
    Stack,
    aws_apigateway,
    Aws,
    aws_iam,
    aws_lambda_event_sources as source,
)
from constructs import Construct
from codereview.database.stack import Database
from codereview.api.stack import API
from codereview.lambda_function.stack import Lambda
from codereview.sqs.stack import SQS
from codereview.bucket.stack import Bucket

# from cdk_nag import AwsSolutionsChecks, NagSuppressions


class CodeReview(Stack):

    def __init__(
        self, scope: Construct, construct_id: str, env_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # Aspects.of(self).add(AwsSolutionsChecks())
        # NagSuppressions.add_stack_suppressions(
        #     self,
        #     suppressions=[
        #         {
        #             "id": "AwsSolutions-IAM5",
        #             "reason": "Suppress useless warnings",
        #         },
        #         {
        #             "id": "AwsSolutions-IAM4",
        #             "reason": "Suppress useless warnings",
        #         },
        #         {
        #             "id": "AwsSolutions-L1",
        #             "reason": "Suppress useless warnings",
        #         },
        #         {"id": "AwsSolutions-L1", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-APIG3", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-APIG2", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-APIG4", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-APIG6", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-COG4", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-SQS3", "reason": "Suppress useless warnings"},
        #         {"id": "AwsSolutions-SQS4", "reason": "Suppress useless warnings"},
        #     ],
        # )

        # parameteres for cdk
        region = Aws.REGION
        account = Aws.ACCOUNT_ID
        # s3 bucket
        bucket = Bucket(
            self, "bucket", account=account, region=region, env_name_string=env_name
        )

        # SQS
        sqs = SQS(self, "task_queue", env_name_string=env_name)

        # dynamodb
        database = Database(self, "database", env_name_string=env_name)

        # ### lambda function
        lambda_functions = Lambda(self, "lambda_functions", env_name_string=env_name)

        bucket.bucket.grant_write(lambda_functions.api_post_codereview)
        bucket.bucket.grant_read_write(lambda_functions.code_review)
        bucket.bucket.grant_read_write(lambda_functions.api_get_result)
        bucket.lambda_log_bucket.grant_write(lambda_functions.api_post_codereview)
        bucket.lambda_log_bucket.grant_write(lambda_functions.code_review)
        bucket.lambda_log_bucket.grant_write(lambda_functions.api_get_result)
        bucket.lambda_log_bucket.grant_write(lambda_functions.split_task)
        bucket.lambda_log_bucket.grant_write(lambda_functions.codereview_get_score_file)
        lambda_functions.api_post_codereview.add_environment(
            "LAMBDA_LOG_BUCKET_NAME", bucket.lambda_log_bucket.bucket_name
        )
        lambda_functions.code_review.add_environment(
            "LAMBDA_LOG_BUCKET_NAME", bucket.lambda_log_bucket.bucket_name
        )
        lambda_functions.api_get_result.add_environment(
            "LAMBDA_LOG_BUCKET_NAME", bucket.lambda_log_bucket.bucket_name
        )
        lambda_functions.split_task.add_environment(
            "LAMBDA_LOG_BUCKET_NAME", bucket.lambda_log_bucket.bucket_name
        )
        lambda_functions.codereview_get_score_file.add_environment(
            "LAMBDA_LOG_BUCKET_NAME", bucket.lambda_log_bucket.bucket_name
        )
        # # grant api_post_codereview lambda function permission
        database.repo_code_review_table.grant_write_data(
            lambda_functions.api_post_codereview
        )
        database.repo_code_review_table.grant_read_write_data(
            lambda_functions.code_review
        )
        database.repo_code_review_table.grant_read_write_data(
            lambda_functions.modify_dynamodb
        )
        database.repo_code_review_score_table.grant_read_write_data(
            lambda_functions.code_review
        )
        database.repo_code_review_score_table.grant_read_write_data(
            lambda_functions.modify_dynamodb
        )
        database.repo_code_review_score_table.grant_read_write_data(
            lambda_functions.codereview_get_score_file
        )
        database.repo_code_review_table.grant_read_data(lambda_functions.api_get_result)
        database.repo_code_review_table.grant_read_write_data(
            lambda_functions.split_task
        )
        lambda_functions.api_post_codereview.add_environment(
            "REPO_CODE_REVIEW_TABLE_NAME", database.repo_code_review_table.table_name
        )

        lambda_functions.split_task.grant_invoke(lambda_functions.api_post_codereview)

        lambda_functions.api_post_codereview.add_environment(
            "CODE_REVIEW_WHITE_LIST", ".py:.go:.cpp:.ts:.js:.c"
        )

        lambda_functions.split_task.add_environment(
            "CODE_REVIEW_WHITE_LIST", ".py:.go:.cpp:.ts:.js:.c"
        )

        lambda_functions.api_post_codereview.add_environment(
            "SPLIT_TASK_LAMBDA_NAME", lambda_functions.split_task.function_name
        )

        lambda_functions.api_get_result.add_environment(
            "REPO_CODE_REVIEW_TABLE_NAME", database.repo_code_review_table.table_name
        )
        lambda_functions.api_get_result.add_environment("EXPIRES_IN", "36000")

        lambda_functions.split_task.add_environment(
            "REPO_CODE_REVIEW_TABLE_NAME", database.repo_code_review_table.table_name
        )

        lambda_functions.api_post_codereview.add_environment(
            "TASK_SQS_URL", sqs.codereview_task_queue.queue_url
        )

        lambda_functions.split_task.add_environment(
            "TASK_SQS_URL", sqs.codereview_task_queue.queue_url
        )
        lambda_functions.split_task.add_environment("FILE_SIZE_LIMIT", "102400")
        lambda_functions.split_task.add_environment("FILE_NUM_LIMIT", "3000")

        lambda_functions.code_review.add_environment(
            "TASK_SQS_URL", sqs.codereview_task_queue.queue_url
        )
        bedrock_policy = aws_iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=["*"],
        )
        lambda_functions.code_review.role.add_to_policy(bedrock_policy)
        lambda_functions.code_review.add_environment(
            "REPO_CODE_REVIEW_TABLE_NAME", database.repo_code_review_table.table_name
        )
        lambda_functions.code_review.add_environment(
            "REPO_CODE_REVIEW_SCORE_TABLE_NAME", database.repo_code_review_score_table.table_name
        )
        lambda_functions.codereview_get_score_file.add_environment(
            "REPO_CODE_REVIEW_SCORE_TABLE_NAME", database.repo_code_review_score_table.table_name
        )
        net_policy = aws_iam.PolicyStatement(
            actions=[
                "ec2:DescribeNetworkInterfaces",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeInstances",
                "ec2:AttachNetworkInterface",
            ],
            resources=["*"],
        )
        lambda_functions.api_post_codereview.role.add_to_policy(net_policy)
        lambda_functions.split_task.role.add_to_policy(net_policy)
        lambda_functions.api_get_result.role.add_to_policy(net_policy)
        lambda_functions.code_review.role.add_to_policy(net_policy)
        lambda_functions.codereview_get_score_file.role.add_to_policy(net_policy)

        lambda_functions.code_review.add_environment(
            "BUCKET_NAME", bucket.bucket.bucket_name
        )
        lambda_functions.code_review.add_environment(
            "LLM_ID", "anthropic.claude-3-sonnet-20240229-v1:0"
        )
        lambda_functions.code_review.add_environment("TEMPERATURE", "0.1")
        lambda_functions.code_review.add_environment("TOP_P", "0.9")
        lambda_functions.code_review.add_environment("MAX_TOKEN_TO_SAMPLE", "10000")
        lambda_functions.code_review.add_environment("MAX_FAILED_TIMES", "6")
        lambda_functions.api_get_result.add_environment(
            "BUCKET_NAME", bucket.bucket.bucket_name
        )
        lambda_functions.modify_dynamodb.add_environment(
            "REPO_CODE_REVIEW_SCORE_TABLE_NAME", "repo_code_review_score_dev2"
        )
        lambda_functions.modify_dynamodb.add_environment(
            "REPO_CODE_REVIEW_TABLE_NAME", "repo_code_review_table_dev2"
        )
        sqs.codereview_task_queue.grant_send_messages(lambda_functions.split_task)
        sqs.codereview_task_queue.grant_consume_messages(lambda_functions.code_review)
        sqs.codereview_task_queue.grant_send_messages(lambda_functions.code_review)
        sqs_event_source = source.SqsEventSource(
            sqs.codereview_task_queue, batch_size=1
        )
        lambda_functions.code_review.add_event_source(sqs_event_source)

        # api gateway
        api = API(self, "api", env_name_string=env_name)

        api_codereview_resource = api.api.root.add_resource("codereview")

        api_post_codereview_integration = aws_apigateway.LambdaIntegration(
            lambda_functions.api_post_codereview
        )
        api_codereview_resource.add_method(
            "POST", api_post_codereview_integration, api_key_required=True
        )
        api_result = api.api.root.add_resource("getReviewResult")
        api_get_result_integration = aws_apigateway.LambdaIntegration(
            lambda_functions.api_get_result
        )
        api_result.add_method("POST", api_get_result_integration, api_key_required=True)

        api_records = api.api.root.add_resource("getReviewRecords")

        api_records.add_method(
            "POST", api_get_result_integration, api_key_required=True
        )

        api_get_score_file = api.api.root.add_resource("getScoreFile")

        api_get_score_file_integration = aws_apigateway.LambdaIntegration(
            lambda_functions.codereview_get_score_file
        )
        
        api_get_score_file.add_method("POST", api_get_score_file_integration, api_key_required=True)

        api_get_score_file = api.api.root.add_resource("getReviewFiles")
        
        api_get_score_file.add_method("POST", api_get_score_file_integration, api_key_required=True)

        api_get_score_file = api.api.root.add_resource("getFileRecords")
        
        api_get_score_file.add_method("POST", api_get_score_file_integration, api_key_required=True)

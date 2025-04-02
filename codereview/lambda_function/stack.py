from aws_cdk import (
    Duration,
    aws_lambda,
    aws_iam,
    aws_apigateway,
    aws_logs,
)
from aws_cdk import aws_ecr_assets as ecr_assets
from constructs import Construct


class Lambda(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, env_name_string: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # git lab lambda layers

        gitlabpython_layer = aws_lambda.LayerVersion(
            self,
            "gitlabpython_layer",
            code=aws_lambda.Code.from_asset("codereview/lambda_layer/gitlab"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            layer_version_name="gitlabpython_layer_{}".format(env_name_string),
        )

        # boto3 lambda layers

        boto3python_layer = aws_lambda.LayerVersion(
            self,
            "boto3python_layer",
            code=aws_lambda.Code.from_asset("codereview/lambda_layer/boto3-new"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            layer_version_name="boto3python_layer_{}".format(env_name_string),
        )

        # jinja2 lambda layers

        jinja2python_layer = aws_lambda.LayerVersion(
            self,
            "jinja2python_layer",
            code=aws_lambda.Code.from_asset("codereview/lambda_layer/jinja2"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            layer_version_name="jinja2python_layer_{}".format(env_name_string),
        )

        # pygments lambda layers

        pygmentspython_layer = aws_lambda.LayerVersion(
            self,
            "pygmentspython_layer",
            code=aws_lambda.Code.from_asset("codereview/lambda_layer/pygments"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_11],
            layer_version_name="pygmentspython_layer_{}".format(env_name_string),
        )
        # code review post lambda function

        self.api_post_codereview = aws_lambda.Function(
            self,
            "api_post_code_review",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(900),
            memory_size=4096,
            code=aws_lambda.Code.from_asset(
                "codereview/lambda_function/api_post_code_review"
            ),
            handler="lambda_function.lambda_handler",
            function_name="code_review_post_{}".format(env_name_string),
            layers=[gitlabpython_layer],
        )

        # split task lambda function

        self.split_task = aws_lambda.Function(
            self,
            "split_task",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(900),
            memory_size=4096,
            code=aws_lambda.Code.from_asset("codereview/lambda_function/split_task"),
            handler="lambda_function.lambda_handler",
            function_name="split_task_{}".format(env_name_string),
            layers=[gitlabpython_layer],
        )

        # get result lambda function

        self.api_get_result = aws_lambda.Function(
            self,
            "api_get_result",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(40),
            memory_size=4096,
            code=aws_lambda.Code.from_asset(
                "codereview/lambda_function/api_get_result"
            ),
            handler="lambda_function.lambda_handler",
            function_name="get_result_{}".format(env_name_string),
            layers=[],
        )

        # code review lambda function

        self.code_review = aws_lambda.Function(
            self,
            "code_review",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(900),
            memory_size=4096,
            code=aws_lambda.Code.from_asset("codereview/lambda_function/code_review"),
            handler="lambda_function.lambda_handler",
            function_name="code_review_{}".format(env_name_string),
            layers=[boto3python_layer, jinja2python_layer, pygmentspython_layer],
        )

        # Project Score lambda function

        self.codereview_get_score_file = aws_lambda.Function(
            self,
            "codereview_get_score_file",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(900),
            memory_size=4096,
            code=aws_lambda.Code.from_asset("codereview/lambda_function/codereview_get_score_file"),
            handler="lambda_function.lambda_handler",
            function_name="codereview_get_score_file_{}".format(env_name_string),
            layers=[boto3python_layer, jinja2python_layer, pygmentspython_layer],
        )

        # Modify dynamodb records function

        self.modify_dynamodb = aws_lambda.Function(
            self,
            "modify_dynamodb",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(900),
            memory_size=4096,
            code=aws_lambda.Code.from_asset("codereview/lambda_function/modify_dynamodb"),
            handler="lambda_function.lambda_handler",
            function_name="modify_dynamodb_{}".format(env_name_string),
        )

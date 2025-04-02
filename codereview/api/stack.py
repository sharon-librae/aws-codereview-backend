from aws_cdk import (
    aws_apigateway,
    aws_logs,
    aws_iam,
)
from constructs import Construct
from aws_cdk import aws_ec2
import aws_cdk


class API(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, env_name_string: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        api_log_group = aws_logs.LogGroup(
            self,
            "code_review_api_log_group",
            log_group_name="codereview_api_log_group_{}".format(env_name_string),
        )

        endpoint_configuration = aws_apigateway.EndpointConfiguration(
            types=[aws_apigateway.EndpointType.REGIONAL]
        )
        resource_policy = aws_iam.PolicyDocument(
            statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=["execute-api:Invoke"],
                    resources=["execute-api:/*"],
                    principals=[aws_iam.StarPrincipal()],
                )
            ]
        )
        self.api = aws_apigateway.RestApi(
            self,
            "codereview_api",
            rest_api_name="codereview_api_{}".format(env_name_string),
            description="codereview api",
            cloud_watch_role=True,
            deploy=True,
            default_cors_preflight_options=aws_apigateway.CorsOptions(
                allow_origins=aws_apigateway.Cors.ALL_ORIGINS
            ),
            endpoint_configuration=endpoint_configuration,
            deploy_options=aws_apigateway.StageOptions(
                access_log_destination=aws_apigateway.LogGroupLogDestination(
                    api_log_group
                ),
                access_log_format=aws_apigateway.AccessLogFormat.clf(),
                throttling_burst_limit=10,
                throttling_rate_limit=10,
            ),
            policy=resource_policy,
        )

        # If it's public API, add api key and usage plan

        api_key = self.api.add_api_key(
            "codereview_api_key",
            api_key_name="codereview_api_key_{}".format(env_name_string),
            description="codereview api key",
        )

        usage_plan = self.api.add_usage_plan(
            "codereview_api_usage_plan",
            name="codereview_api_usage_plan_{}".format(env_name_string),
            description="codereview api usage plan",
            throttle=aws_apigateway.ThrottleSettings(rate_limit=10, burst_limit=10),
            quota=aws_apigateway.QuotaSettings(
                limit=10000, period=aws_apigateway.Period.DAY
            ),
        )

        usage_plan.add_api_stage(
            stage=self.api.deployment_stage,
        )
        usage_plan.add_api_key(api_key)
        aws_cdk.CfnOutput(
            self,
            "codereview_api_key_output",
            value=api_key.key_id,
            description="codereview api key",
        )

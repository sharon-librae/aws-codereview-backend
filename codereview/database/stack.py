import aws_cdk as cdk
from aws_cdk import (
    aws_lambda,
    aws_lambda_event_sources as source,
)
from aws_cdk.aws_dynamodb import (
    Table,
    TableEncryption,
    Attribute,
    AttributeType,
    BillingMode,
    StreamViewType,
    ProjectionType,
    GlobalSecondaryIndexProps,
)

from constructs import Construct


class Database(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, env_name_string: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ### request table
        self.repo_code_review_table = Table(
            self,
            "repo_code_review_table_{}".format(env_name_string),
            table_name="repo_code_review_table_{}".format(env_name_string),
            partition_key=Attribute(name="review_id", type=AttributeType.STRING),
            billing_mode=BillingMode.PAY_PER_REQUEST,
            encryption=TableEncryption.AWS_MANAGED,
            stream=StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True,
        )
        # Add the Global Secondary Index to the table

        self.repo_code_review_table.add_global_secondary_index(
            index_name="project_created_at_gsi",
            partition_key=Attribute(name="project", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )

        self.repo_code_review_table.add_global_secondary_index(
            index_name="branch_created_at_gsi",
            partition_key=Attribute(name="branch", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )

        self.repo_code_review_table.add_global_secondary_index(
            index_name="scan_scope_created_at_gsi",
            partition_key=Attribute(name="scan_scope", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )
        self.repo_code_review_table.add_global_secondary_index(
            index_name="commit_id_created_at_gsi",
            partition_key=Attribute(name="commit_id", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )
        self.repo_code_review_table.add_global_secondary_index(
            index_name="repo_url_created_at_gsi",
            partition_key=Attribute(name="repo_url", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )

        self.repo_code_review_table.add_global_secondary_index(
            index_name="year_month-created_at-index",
            partition_key=Attribute(name="year_month", type=AttributeType.STRING),
            sort_key=Attribute(name="created_at", type=AttributeType.STRING),
        )

        # request second dynamodb table
        self.repo_code_review_score_table = Table(
            self,
            "repo_code_review_score_table_{}".format(env_name_string),
            table_name="repo_code_review_score_{}".format(env_name_string),
            partition_key=Attribute(name="project_branch_file", type=AttributeType.STRING),
            sort_key=Attribute(name="version", type=AttributeType.NUMBER),
            billing_mode=BillingMode.PAY_PER_REQUEST,
            encryption=TableEncryption.AWS_MANAGED,
            stream=StreamViewType.NEW_IMAGE,
            point_in_time_recovery=True,
        )

        # Add the Global Secondary Index to the table
        self.repo_code_review_score_table.add_global_secondary_index(
            index_name="version_file_index",
            partition_key=Attribute(name="version", type=AttributeType.NUMBER),
            sort_key=Attribute(name="project_branch_file", type=AttributeType.STRING),
        )

        self.repo_code_review_score_table.add_global_secondary_index(
            index_name="file_review_at_gsi",
            partition_key=Attribute(name="project_branch_file", type=AttributeType.STRING),
            sort_key=Attribute(name="review_at", type=AttributeType.STRING),
        )

        self.repo_code_review_score_table.add_global_secondary_index(
            index_name="review_id_gsi",
            partition_key=Attribute(name="review_id", type=AttributeType.STRING),
        )
        self.repo_code_review_score_table.add_global_secondary_index(
            index_name="year_month-review_at-index",
            partition_key=Attribute(name="year_month", type=AttributeType.STRING),
            sort_key=Attribute(name="review_at", type=AttributeType.STRING),
        )
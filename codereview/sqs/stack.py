from aws_cdk import (
    aws_sqs as sqs,
    Duration
)
from constructs import Construct

class SQS(Construct):
    def __init__(self, scope: Construct, construct_id: str, env_name_string: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.codereview_task_queue = sqs.Queue(
            self, "codereview_task_queue",
            queue_name="codereview_task_queue_{}".format(env_name_string),
            visibility_timeout=Duration.minutes(20),
            encryption=sqs.QueueEncryption.KMS_MANAGED,
            )
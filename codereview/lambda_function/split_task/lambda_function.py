import json
import boto3
import os
from datetime import datetime, timedelta
import gitlab
import logging


# Initialize AWS services clients
SQS_CLIENT = boto3.client("sqs")
DYNAMODB = boto3.resource("dynamodb")

# Environment variables and constants
CODE_REVIEW_WHITE_LIST = os.getenv("CODE_REVIEW_WHITE_LIST", ".py:.go:.cpp:.ts:.c:.js")
SQS_URL = os.getenv("TASK_SQS_URL")
REPO_CODE_REVIEW_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_TABLE_NAME")
REPO_CODE_REVIEW_TABLE = DYNAMODB.Table(REPO_CODE_REVIEW_TABLE_NAME)
LLM_STATUS = "InProgress LLM"
GET_FILE_ERROR = "GET FILE ERROR"
LAMBDA_LOG_BUCKET_NAME = os.getenv("LAMBDA_LOG_BUCKET_NAME")
S3 = boto3.client("s3")

logging.basicConfig(
    force=True,
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ui_print(log, lambda_name="lambda_split_task"):
    logging.debug(log)
    timestamp = str(datetime.now())
    msg = f"{timestamp}: {log}\n"
    object_key = f"logs/{lambda_name}_{timestamp}.txt"
    S3.put_object(
        Bucket=LAMBDA_LOG_BUCKET_NAME,
        Key=object_key,
        Body=msg.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )


def str_to_int(s):
    try:
        return int(s)
    except ValueError:
        ui_print(f"The string {s} is not a number.")
        return None


# 100KB default
FILE_SIZE_LIMIT = str_to_int(os.getenv("FILE_SIZE_LIMIT", "102400"))
FILE_NUM_LIMIT = str_to_int(os.getenv("FILE_NUM_LIMIT", "3000"))


def get_selected_content(content, max_size_in_bytes=FILE_SIZE_LIMIT):
    """
    Returns the first max_size_in_bytes of the content.

    Parameters:
    content (str): The content to be trimmed.

    Returns:
    str: The original content if it's less or equal to max_size_in_bytes, or the first max_size_in_bytes of the content.
    """
    if len(content) <= max_size_in_bytes:
        return content.decode("utf-8", errors="ignore")
    else:
        return content[:max_size_in_bytes].decode("utf-8", errors="ignore")


def check_extension(filename, extensions=CODE_REVIEW_WHITE_LIST):
    """
    Checks if the file extension is in the provided list of extensions.

    Parameters:
    extensions (str): A string containing multiple extensions separated by colons.
    filename (str): The filename to check.

    Returns:
    bool: True if the file extension is in the list, False otherwise.
    """
    ext_list = extensions.split(":")
    _, file_extension = os.path.splitext(filename)
    return file_extension in ext_list


def get_file_content(
    project, file_path, ref_name="main", file_size_limit=FILE_SIZE_LIMIT
):
    """
    Attempts to retrieve the content of a file from a GitLab project.

    Parameters:
    project: The GitLab project object.
    file_path (str): The path to the file.
    ref_name (str): The branch or tag to fetch the file from.

    Returns:
    str: The content of the file or an error message.
    """
    try:
        file_content = project.files.get(file_path=file_path, ref=ref_name).decode()
        file_content = get_selected_content(file_content, file_size_limit)
        # ui_print("File content:", file_content)
        return file_content
    except Exception as e:
        ui_print("Error occurred:", e)
    return GET_FILE_ERROR


def send_message(message, sqs_url=SQS_URL):
    try:
        response = SQS_CLIENT.send_message(QueueUrl=sqs_url, MessageBody=message)
        return True
    except Exception as e:
        ui_print("An unexpected error occurred:", e)
        return False


def send_fullscan_task_to_sqs(
    review_id, project, project_idorpath, commit_id, file_list, branch="main"
):
    """
    Processes changes in a commit and sends tasks to SQS.

    Parameters:
    project: The GitLab project object.
    commit_id (str): The ID of the commit.
    file_list(list): The list of the files.

    Returns:
    int: The number of files processed.
    """

    # 用于存储文件路径的数组
    file_paths = []

    # 递归函数，用于遍历目录并收集文件路径
    def list_files(project, path=""):
        items = project.repository_tree(path=path, all=True, recursive=True)
        # ui_print(items)
        for item in items:
            if item["type"] == "blob":  # 如果是文件
                file_paths.append(item["path"])

    file_num = 0
    if file_list == []:
        list_files(project, path="")
    else:
        file_paths = file_list
        
    ui_print(f"file numbers: {len(file_paths)}")
    for file_path in file_paths:
        ui_print(file_path)
        if file_num >= FILE_NUM_LIMIT:
            ui_print(f"Processed {FILE_NUM_LIMIT} messages, stopping.")
            break
        if check_extension(file_path, CODE_REVIEW_WHITE_LIST):
            file_content = get_file_content(project, file_path, branch, FILE_SIZE_LIMIT)
            if file_content == GET_FILE_ERROR:
                continue
            item = {
                "review_id": review_id,
                "project": project_idorpath,
                "branch": branch,
                "commit_id": commit_id,
                "file_list": file_list,
                "file_name": file_path,
                "file_content": file_content,
                "scan_scope": "ALL",
                "msg_type": "file review",
            }
            status = send_message(json.dumps(item))
            if status is True:
                file_num += 1
            else:
                continue
    return file_num
    
    
def check_changes_files(changes):
    files = []
    for change in changes:
        files.append(change["new_path"])
    return files


def send_task_to_sqs(review_id, project, project_idorpath, commit_id, file_list, branch="main"):
    """
    Processes changes in a commit and sends tasks to SQS.

    Parameters:
    project: The GitLab project object.
    commit_id (str): The ID of the commit.
    file_list(list): The list of files need to be reviewed

    Returns:
    int: The number of files processed.
    """
    file_num = 0
    if commit_id != "00000000" and file_list != []:
        commit = project.commits.get(commit_id)
        changes = commit.diff(get_all=True, all=True)
        change_files = list(set(check_changes_files(changes)) & set(file_list))
    elif commit_id != "00000000" and file_list == []:
        commit = project.commits.get(commit_id)
        changes = commit.diff(get_all=True, all=True)
        change_files = check_changes_files(changes)
                    
    for change in changes:
        if change["new_path"] in change_files:
            if file_num >= FILE_NUM_LIMIT:
                ui_print(f"Processed {FILE_NUM_LIMIT} messages, stopping.")
                break
            if check_extension(change["new_path"], CODE_REVIEW_WHITE_LIST):
                file_content = get_file_content(project, change["new_path"], branch)
                if file_content == GET_FILE_ERROR:
                    continue
    
                item = {
                    "review_id": review_id,
                    "project": project_idorpath,
                    "branch": branch,
                    "commit_id": commit_id,
                    "file_list": file_list,
                    "file_name": change["new_path"],
                    "diff": change["diff"],
                    "file_content": file_content,
                    "scan_scope": "DIFF",
                    "msg_type": "file review",
                }
                status = send_message(json.dumps(item))
                if status is True:
                    file_num += 1
                else:
                    continue
    return file_num


def update_dynamodb_status(review_id, status, file_num):
    """
    Updates the status of a commit in DynamoDB.

    Parameters:
    commit_id (str): The ID of the commit.
    status (str): The new status.
    file_num (int): The number of files processed.
    """
    REPO_CODE_REVIEW_TABLE.update_item(
        Key={"review_id": review_id},
        UpdateExpression="set task_status = :s, update_at = :t, file_num = file_num + :m",
        ExpressionAttributeValues={
            ":s": status,
            ":t": str(datetime.now()),
            ":m": file_num,
        },
        ReturnValues="ALL_NEW",
    )


def lambda_handler(event, context):
    """
    The main function to handle the lambda event.

    Parameters:
    event: The event triggering the lambda.
    context: The context in which the lambda is executed.

    Returns:
    dict: The response containing the status and other information.
    """
    # Extract parameters from the event
    review_id = event["review_id"]
    private_token = event["private_token"]
    project_idorpath = event["project_idorpath"]
    repo_url = event["repo_url"]
    commit_id = event["commit_id"]
    file_list = event["file_list"]
    scan_scope = event["scan_scope"]
    branch = event["branch"]
    current_time = datetime.now()

    # Get diff and insert into SQS
    if not repo_url:
        gl = gitlab.Gitlab(private_token=private_token)
    else:
        gl = gitlab.Gitlab(repo_url, private_token=private_token)
    project = gl.projects.get(project_idorpath)
    if scan_scope == "ALL":
        file_num = send_fullscan_task_to_sqs(
            review_id, project, project_idorpath, commit_id, file_list, branch
        )
    else:
        file_num = send_task_to_sqs(
            review_id, project, project_idorpath, commit_id, file_list, branch
        )
    # Update DynamoDB - request
    update_dynamodb_status(review_id, LLM_STATUS, file_num)
    result = {
        "status": "Success",
        "error_message": "",
        "review_id": review_id,
        "timestamp": str(current_time),
    }
    return {"statusCode": 200, "body": json.dumps(result)}

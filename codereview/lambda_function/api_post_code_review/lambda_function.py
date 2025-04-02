import json
import boto3
import os
import re
from datetime import datetime, timedelta
import gitlab
import hashlib
import logging


DYNAMODB = boto3.resource("dynamodb")
LAMBDA_CLIENT = boto3.client("lambda")
REPO_CODE_REVIEW_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_TABLE_NAME")
REPO_CODE_REVIEW_TABLE = DYNAMODB.Table(REPO_CODE_REVIEW_TABLE_NAME)

SPLIT_TASK_LAMBDA_NAME = os.getenv("SPLIT_TASK_LAMBDA_NAME")
CODE_REVIEW_WHITE_LIST = os.getenv("CODE_REVIEW_WHITE_LIST", ".py:.go:.cpp:.ts")
COMPLETED_STATUS = "Completed"
LAMBDA_LOG_BUCKET_NAME = os.getenv("LAMBDA_LOG_BUCKET_NAME")
S3 = boto3.client("s3")

logging.basicConfig(
    force=True,
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ui_print(log, lambda_name="lambda_api_post_code_review"):
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


def generate_unique_key(repo_url, commit_id, file_list, scan_scope, project, branch):
    """
    Generates a unique key for DynamoDB item using SHA-256 hashing.

    Parameters:
    - repo_url (str)
    - commit_id (str): The ID of the commit.
    - file_list(list): The list of the files.
    - scan_scope (str): The scope of the scan.
    - project (str): The name of the project.
    - branch (str): The name of the branch.

    Returns:
    - str: A unique key for the DynamoDB item.
    """
    
    if file_list != []:
        file_list_string = ''.join(file_list)
    
    # Concatenate the parameters with a delimiter to form the base string
    if commit_id != "00000000" and file_list == []:
        base_string = f"{repo_url}:{commit_id}:{scan_scope}:{project}:{branch}"
    elif commit_id == "00000000" and file_list != []:
        base_string = f"{repo_url}:{file_list_string}:{scan_scope}:{project}:{branch}:{str(datetime.now())}"
    elif commit_id != "00000000" and file_list != []:
        base_string = f"{repo_url}:{commit_id}:{file_list_string}:{scan_scope}:{project}:{branch}:"

    # Encode the base string to bytes
    base_bytes = base_string.encode("utf-8")

    # Create a SHA-256 hash object and update it with the base bytes
    hash_object = hashlib.sha256(base_bytes)

    # Get the hexadecimal representation of the hash
    unique_key = hash_object.hexdigest()

    return unique_key


def check_extension(filename, extensions=CODE_REVIEW_WHITE_LIST):
    """
    检查文件名的扩展名是否在传入的扩展名列表中。

    参数:
    extensions (str): 包含多个扩展名的字符串，各个扩展名之间用冒号分隔。
    filename (str): 要检查的文件名。

    返回:
    bool: 如果文件名的扩展名在传入的列表中，则返回True，否则返回False。
    """
    # 将传入的扩展名字符串分割成列表
    ext_list = extensions.split(":")

    # 获取文件名的扩展名
    _, file_extension = os.path.splitext(filename)
    
    print("file_extension is " + file_extension)

    # 检查文件名的扩展名是否在列表中
    return file_extension in ext_list


def contains_extension(changes):
    """
    Check if the changes contain any files with the .go/.py... extension.

    Parameters:
    changes (list): A list of change items from GitLab commit diff.

    Returns:
    bool: True if there is at least one .go/.py...  file, False otherwise.
    """
    for change in changes:
        if check_extension(change, CODE_REVIEW_WHITE_LIST):
            return True
    return False


def insert_dynamodb(item):
    try:
        # 尝试向DynamoDB表中插入项目
        response = REPO_CODE_REVIEW_TABLE.put_item(Item=item)
        return response
    except Exception as e:
        # 捕获其他可能的异常
        ui_print(f"An unexpected error occurred: {str(e)}")
        return None


def retrun_data(status, message):
    current_time = datetime.now()

    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "status": status,
                "timestamp": str(current_time),
                "message": message,
            }
        ),
    }
    ui_print(response)
    return response


def get_request_item(
    review_id,
    repo_url,
    commit_id,
    file_list,
    scan_scope,
    project,
    branch,
    current_time,
    task_status="InProgress",
    file_num=0,
    file_done=0,
):
    return {
        "review_id": review_id,
        "repo_url": repo_url,
        "commit_id": commit_id,
        "file_list": file_list,
        "scan_scope": scan_scope,
        "project": project,
        "branch": branch,
        "file_num": file_num,
        "file_done": file_done,
        "task_status": task_status,
        "file_review_html_key": "",
        "review_summary_html_key": "",
        "created_at": str(current_time),
        "update_at": str(current_time),
        "year_month": str(current_time.strftime('%Y-%m')),
    }
    
def check_changes_files(changes):
    files = []
    for change in changes:
        files.append(change["new_path"])
    return files


def lambda_handler(event, context):
    body = json.loads(event["body"])
    ui_print(body)
    # 1.get var
    try:
        # get parameter from request    
        repo_url = body["repo_url"]
        project_idorpath = body["project"]
        access_token = body["access_token"]
        scan_scope = "DIFF"
        branch = "main"
        
        # check commitid and filelist
        if "commitid" in body:
            commit_id = body["commitid"]
        else:
            commit_id = "00000000"
        if "filelist" in body:
            file_list = list(body["filelist"])
        else:
            file_list = []
        
        if "branch" in body:
            branch = str(body["branch"])
        if "scan_scope" in body:
            scan_scope = str(body["scan_scope"])
        current_time = datetime.now()

        review_id = generate_unique_key(
            repo_url, commit_id, file_list, scan_scope, project_idorpath, branch
        )
            
        result = {
            "status": "success",
            "error_message": "",
            "review_id": review_id,
            "repo_url": repo_url,
            "commitid": commit_id,
            "file_list": file_list,
            "scan_scope": scan_scope,
            "project": project_idorpath,
            "branch": branch,
            "timestamp": str(current_time),
        }

        # 2.get diff and insert into sqs todo
        private_token = access_token
        if not repo_url:
            gl = gitlab.Gitlab(private_token=private_token)
        else:
            gl = gitlab.Gitlab(repo_url, private_token=private_token)
        project = gl.projects.get(project_idorpath)
        try:
            change_files = []
            if scan_scope == "DIFF":
                if commit_id != "00000000" and file_list != []:
                    commit = project.commits.get(commit_id)
                    changes = commit.diff(get_all=True, all=True)
                    change_files = list(set(check_changes_files(changes)) & set(file_list))
                    print("change file is: " + str(change_files))
                elif commit_id == "00000000" and file_list != []:
                    return retrun_data(status="failure", message="DIFF must insert commit_id")
                elif commit_id != "00000000" and file_list == []:
                    commit = project.commits.get(commit_id)
                    changes = commit.diff(get_all=True, all=True)
                    change_files = check_changes_files(changes)
                    
                if contains_extension(change_files) == False:
                    request_item = get_request_item(
                        review_id,
                        repo_url,
                        commit_id,
                        file_list,
                        scan_scope,
                        project_idorpath,
                        branch,
                        current_time,
                        task_status=COMPLETED_STATUS,
                    )
                    insert_dynamodb(request_item)
                    return retrun_data(status="failure", message="No file need review")
        except Exception as e:
            ui_print(str(e))
            return retrun_data(status="failure", message=str(e))
        # 3. Insert dynamodb - request]
        request_item = get_request_item(
            review_id,
            repo_url,
            commit_id,
            file_list,
            scan_scope,
            project_idorpath,
            branch,
            current_time,
        )
        insert_dynamodb(request_item)
        
        # 定义要传递给第二个Lambda函数的参数
        payload = {
            "review_id": review_id,
            "private_token": private_token,
            "project_idorpath": project_idorpath,
            "repo_url": repo_url,
            "commit_id": commit_id,
            "file_list": file_list,
            "scan_scope": scan_scope,
            "branch": branch,
        }

        # 调用第二个Lambda函数
        LAMBDA_CLIENT.invoke(
            FunctionName=SPLIT_TASK_LAMBDA_NAME,
            InvocationType="Event",  # 使用'Event'进行异步调用
            Payload=json.dumps(payload),
        )

    except Exception as e:
        ui_print(str(e))
        return retrun_data(status="failure", message=str(e))
    res = {"statusCode": 200, "body": json.dumps(result)}
    ui_print(res)
    return res

import boto3
import logging
import os
import json
from datetime import datetime, timedelta
from jinja2 import Template, Environment, select_autoescape
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from pygments import highlight
from pygments.lexers import get_lexer_for_filename
from pygments.formatters import HtmlFormatter
from boto3.dynamodb.conditions import Key
import re


BEDROCK = boto3.client(service_name="bedrock-runtime")
LAMBDA_LOG_BUCKET_NAME = os.getenv("LAMBDA_LOG_BUCKET_NAME")
client_config = Config(max_pool_connections=50)
S3 = boto3.client("s3", config=client_config)
SQS = boto3.client("sqs")
DYNAMODB = boto3.resource("dynamodb")
REPO_CODE_REVIEW_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_TABLE_NAME")
REPO_CODE_REVIEW_TABLE = DYNAMODB.Table(REPO_CODE_REVIEW_TABLE_NAME)
REPO_CODE_REVIEW_SCORE_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_SCORE_TABLE_NAME")
REPO_CODE_REVIEW_SCORE_TABLE = DYNAMODB.Table(REPO_CODE_REVIEW_SCORE_TABLE_NAME)
BEDROCK_ERROR_MSG = "An error occurred: in invoke bedrock."
TASK_SQS_URL = os.getenv("TASK_SQS_URL")
ALL_SCAN_SCOPE = "ALL"
File_REVIEW = "file review"
REVIEW_SUMMARY = "review summary"
HTML_GEN_ERROR = "An error occurred in generating html"
HTML_POSTFIX = "merged-code-review-result.html"
SUMMARY_HTML_POSTFIX = "summary-review-result.html"
COMPLETED_STATUS = "Completed"

NO_FILE_NEED_REVIEW = "No file need review"

MAX_TOKEN_NUM = 150000

REVIEW_SCORE_BAR = 80

logging.basicConfig(
    force=True,
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ui_print(log, lambda_name="lambda_code_review"):
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


def extract_tags(text):
    try:
        score_pattern = re.compile(r"<review_score>(.*?)</review_score>", re.DOTALL)
        result_pattern = re.compile(r"<review_result>(.*?)</review_result>", re.DOTALL)

        score_match = score_pattern.search(text)
        result_match = result_pattern.search(text)

        score = score_match.group(1).strip() if score_match else None
        result = result_match.group(1).strip() if result_match else None
        if score is not None:
            try:
                score = int(score)
            except Exception as val_error:
                print(f"Value error: {val_error}")
                score = 0

        return score, result
    except Exception as e:
        print(f"extract tags error: {e}")
        return 0, ""


def get_request_item(
    project_branch_file,
    version,
    latest,
    update_time,
    review_id,
    commit_id,
    score,
    review_result
):
    return {
        "project_branch_file": str(project_branch_file),
        "version": version,
        "latest": latest,
        "review_at": str(update_time),
        "review_id": review_id,
        "commit_id": commit_id,
        "score": score,
        "review_result": review_result,
        "year_month": str(update_time.strftime('%Y-%m'))
    }


def insert_dynamodb_v0(item):
    try:
        REPO_CODE_REVIEW_SCORE_TABLE.update_item(
            Key={"project_branch_file": item['project_branch_file'], "version": item['version'] },
            UpdateExpression="set latest= if_not_exists(latest, :n), review_at= if_not_exists(review_at, :t), review_id= if_not_exists(review_id, :d), commit_id= if_not_exists(commit_id, :i), score= if_not_exists(score, :s), review_result= if_not_exists(review_result, :r), year_month= if_not_exists(year_month, :y)",
            ExpressionAttributeValues={":n": item["latest"], ":t": str(datetime.now()), ":d" : item["review_id"], ":i": item["commit_id"], ":s": item["score"], ":r": item["review_result"], ":y": str(datetime.now().strftime('%Y-%m'))},
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")
        

def insert_dynamodb(item):
    try:
        # 尝试向DynamoDB表中插入项目
        response = REPO_CODE_REVIEW_SCORE_TABLE.put_item(Item=item)
        return response
    except Exception as e:
        # 捕获其他可能的异常
        ui_print(f"An unexpected error occurred: {str(e)}")
        return None


def update_dynamodb_version(item):
    try:
        REPO_CODE_REVIEW_SCORE_TABLE.update_item(
            Key={"project_branch_file": item['project_branch_file'], "version": item['version'] },
            UpdateExpression="set latest= :n, review_at= :t, review_id= :d, commit_id= :i, score= :s, review_result= :r, year_month= :y",
            ExpressionAttributeValues={":n": item["latest"], ":t": str(datetime.now()), ":d" : item["review_id"], ":i": item["commit_id"], ":s": item["score"], ":r": item["review_result"], ":y": str(datetime.now().strftime('%Y-%m'))},
            ReturnValues="ALL_NEW",
        )
        
    except Exception as e:
        ui_print(f"An error occurred: {e}")
        

def update_dynamodb_only_version(item):
    try:
        REPO_CODE_REVIEW_SCORE_TABLE.update_item(
            Key={"project_branch_file": item['project_branch_file'], "version": item['version'] },
            UpdateExpression="set latest= :n",
            ExpressionAttributeValues={":n": item["latest"]},
            ReturnValues="ALL_NEW",
        )
        
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def get_file_status_recode(project_branch_file, version):
    try:
        print("Start query: " + project_branch_file)
        
        # 执行查询
        response = REPO_CODE_REVIEW_SCORE_TABLE.get_item(
            Key={"project_branch_file": project_branch_file, "version": version},
        )
        print("Completed get status record " + project_branch_file)
        return response
    except Exception as e:
        # 捕获其他可能的异常
        ui_print(f"An unexpected error occurred: {str(e)}")
        return None


def update_dynamodb_file_num(review_id):
    try:
        REPO_CODE_REVIEW_TABLE.update_item(
            Key={"review_id": review_id},
            UpdateExpression="set file_num = file_num - :s, update_at = :t",
            ExpressionAttributeValues={":s": 1, ":t": str(datetime.now())},
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def update_dynamodb_done_file(review_id):
    try:
        REPO_CODE_REVIEW_TABLE.update_item(
            Key={"review_id": review_id},
            UpdateExpression="set file_done = file_done + :s, update_at = :t",
            ExpressionAttributeValues={":s": 1, ":t": str(datetime.now())},
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def update_dynamodb_file_review_html_key(review_id, file_review_html_key, scores):
    try:
        REPO_CODE_REVIEW_TABLE.update_item(
            Key={"review_id": review_id},
            UpdateExpression="set file_review_html_key = :h, min_score = :min, max_score = :max, avg_score= :avg",
            ExpressionAttributeValues={
                ":h": file_review_html_key,  # Assuming html_key_value is the value you want to set for file_review_html_key
                ":min": scores["min_score"],
                ":max": scores["max_score"],
                ":avg": scores["avg_score"],
            },
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def update_dynamodb_review_summary_html_key(review_id, review_summary_html_key):
    try:
        REPO_CODE_REVIEW_TABLE.update_item(
            Key={"review_id": review_id},
            UpdateExpression="set review_summary_html_key = :h",
            ExpressionAttributeValues={
                ":h": review_summary_html_key  # Assuming html_key_value is the value you want to set for file_review_html_key
            },
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def update_dynamodb_stask_status(review_id, task_status=COMPLETED_STATUS):
    try:
        REPO_CODE_REVIEW_TABLE.update_item(
            Key={"review_id": review_id},
            UpdateExpression="set task_status = :h",
            ExpressionAttributeValues={
                ":h": task_status  # Assuming html_key_value is the value you want to set for file_review_html_key
            },
            ReturnValues="ALL_NEW",
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")


def query_dynamodb_by_review_id(review_id):
    try:
        response = REPO_CODE_REVIEW_TABLE.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("review_id").eq(
                review_id
            )
        )
        return response["Items"]
    except Exception as e:
        ui_print(f"An error occurred: {e}")
        return None


def can_merge_review_result(record):
    file_done = int(record["file_done"])
    file_num = int(record["file_num"])
    if file_num != 0 and file_num == file_done:
        return True
    return False


def str_to_float(s):
    try:
        return float(s)
    except ValueError:
        ui_print(f"The string {s} is not a number.")
        return None


def str_to_int(s):
    try:
        return int(s)
    except ValueError:
        ui_print(f"The string {s} is not a number.")
        return None


# "anthropic.claude-3-sonnet-20240229-v1:0"
LLM_ID = os.getenv("LLM_ID", "anthropic.claude-3-sonnet-20240229-v1:0")
TEMPERATURE = os.getenv("TEMPERATURE", "0.1")
TOP_P = os.getenv("TOP_P", "0.9")
MAX_TOKEN_TO_SAMPLE = os.getenv("MAX_TOKEN_TO_SAMPLE", "10000")
TEMPERATURE = str_to_float(TEMPERATURE)
TOP_P = str_to_float(TOP_P)
MAX_TOKEN_TO_SAMPLE = str_to_int(MAX_TOKEN_TO_SAMPLE)
BUCKET_NAME = os.getenv("BUCKET_NAME")
MAX_FAILED_TIMES = str_to_int(os.getenv("MAX_FAILED_TIMES", "6"))


def get_diff_scan_prompt(file_content, file_diff):
    prompt = """You are a code review master. Please provide a concise summary of the bug and vulnerability issue found in the code, describing its characteristics, location, and potential effects on the overall functionality and performance of the application.
    Also provide your code suggestion if there is a more time efficient or memory efficient way to implement the same functionality.
    I would appreciate any feedback you can provide to help me improve my coding skills. Please let me know if you need any clarification or additional context about the code changes.
    Important: Include block of code / diff in the summary.
    And you should score the complete code, the best code scores 100, and the worst code socore 0.
    Here is the complete code in file of the commit:
    <code>
    {file_content}
    </code>

    Below is the code diff of the commit:
    <diff>
    {file_diff}
    </diff>
    **Respond in valid XML format with the tags as "review_score", "review_result"**. 
    Here is one   sample:
    <review_score>
    30
    <\review_score>
    <review_result>
    "这个代码中存在一个明显的 bug"
    <\review_result>
    请使用中文回答
    
    """
    full_prompt = prompt.format(file_content=file_content, file_diff=file_diff)
    return full_prompt


def get_full_scan_prompt(file_content):
    prompt = """You are a code review master. Analyze the code 
    <code>
    {file_content}
    </code>
    and provide a concise summary of the bug and vulnerability issue found in the code, describing its characteristics, location, and potential effects on the overall functionality and performance of the application.
    Also provide your code suggestion if there is a more time efficient or memory efficient way to implement the same functionality.
    And you should score the complete code, the best code scores 100, and the worst code socore 0.
    If there is no bug, just reply "没有发现bug"​ **Do not write explanations.**
    **Respond in valid XML format with the tags as "review_score", "review_result"**. 
    Here is one   sample:
    <review_score>
    30
    <\review_score>
    <review_result>
    "这个代码中存在一个明显的 bug"
    <\review_result>
    请使用中文回答,回答要简洁
    
    """
    full_prompt = prompt.format(file_content=file_content)
    return full_prompt


def gen_records(json_data):
    records = []
    record_template = """
    <file_name>{file_name}</file_name> 
    <file_review_result>{file_review_result}</file_review_result> """

    total_tokens = 0

    for item in json_data:
        if item["review_score"] >= REVIEW_SCORE_BAR:
            continue

        total_tokens += item["output_tokens"]
        if total_tokens > MAX_TOKEN_NUM:
            break

        record = record_template.format(
            file_name=item["file_name"], file_review_result=item["review_result"]
        )
        records.append(record)
        ui_print(
            f"token_nums: {total_tokens}, item output_tokens: {item['output_tokens']}"
        )

    final_records = "".join(records)
    ui_print(final_records)
    return final_records


def gen_review_summary_prompt(json_data):
    prompt_start = """You are a code review result summary master. Please tell me the total number of bug issues and the number of vulnerability issues in the gitlab reposity.
And list the whole file path accordingly.

    Here is the review result of each file in the reposity:    
    
    """
    prompt_end = """
 请使用中文回答
    """
    records = gen_records(json_data)
    return prompt_start + records + prompt_end


def get_full_prompt(scan_scope, file_content, file_diff):
    if scan_scope == ALL_SCAN_SCOPE:
        return get_full_scan_prompt(file_content)
    else:
        return get_diff_scan_prompt(file_content, file_diff)


def invoke_claude3(prompt):
    body = json.dumps(
        {
            "max_tokens": MAX_TOKEN_TO_SAMPLE,
            "temperature": TEMPERATURE,
            "top_p": TOP_P,
            "messages": [{"role": "user", "content": prompt}],
            "anthropic_version": "bedrock-2023-05-31",
        }
    )
    reply = BEDROCK_ERROR_MSG
    output_tokens = 0
    try:
        response = BEDROCK.invoke_model(body=body, modelId=LLM_ID)
        response_body = json.loads(response.get("body").read())
        reply = response_body.get("content")[0]["text"]
        output_tokens = response_body["usage"]["output_tokens"]
        ui_print(f"Bedrock reply: {reply}")
    except Exception as e:
        # Code to handle the error
        err_str = str(e)
        ui_print(f"An error occurred: {err_str}")
        reply = BEDROCK_ERROR_MSG
    return reply, output_tokens


def invoke_bedrock(full_prompt):
    reply = BEDROCK_ERROR_MSG
    if "claude-3" in LLM_ID:
        reply, output_tokens = invoke_claude3(full_prompt)
    return reply, output_tokens


def increment_field(json_obj, field_name):
    """
    检查字段是否存在，如果存在则将其值增加1，如果不存在则创建该字段并设置为1。

    参数:
    json_obj (dict): JSON对象（通常是从json.loads()得到的字典）。
    field_name (str): 要检查和增加的字段名称。

    返回:
    dict: 更新后的JSON对象。
    """
    # 检查字段是否存在
    if field_name in json_obj:
        # 如果字段存在，增加其值
        json_obj[field_name] += 1
    else:
        # 如果字段不存在，创建字段并设置值为1
        json_obj[field_name] = 1
    return json_obj


def get_field(json_obj, field_name):
    # 检查字段是否存在
    if field_name not in json_obj:
        json_obj[field_name] = 0
    return json_obj[field_name]


def get_json_name(scan_scope, project, branch, commit_id, file_name):
    if commit_id != "00000000":
        return (
            scan_scope
            + "/"
            + project
            + "/"
            + branch
            + "/"
            + commit_id
            + "/"
            + file_name
            + "_review_result.json"
        )
    else:
        return (
            scan_scope
            + "/"
            + project
            + "/"
            + branch
            + "/"
            + file_name
            + "_review_result.json"
        )


def extract_message_details(msg_body):
    return (
        msg_body["review_id"],
        msg_body["project"],
        msg_body["branch"],
        msg_body["commit_id"],
        msg_body["file_list"],
        msg_body["file_name"],
        msg_body["file_content"],
        msg_body["scan_scope"],
    )


def extract_file_diff(msg_body, scan_scope):
    return msg_body["diff"] if scan_scope != ALL_SCAN_SCOPE else ""


def process_successful_reply(
    reply,
    output_tokens,
    scan_scope,
    commit_id,
    file_name,
    file_content,
    file_diff,
    project,
    branch,
    review_id,
):
    review_score, review_result = extract_tags(reply)
    score_str = f"review_score: {str(review_score)}\n\n"
    review_result = score_str + review_result
    code_review_result = {
        "commit_id": commit_id,
        "file_name": file_name,
        "review_result": review_result,
        "review_score": review_score,
        "output_tokens": output_tokens,
    }
    if scan_scope != ALL_SCAN_SCOPE:
        code_review_result["diff"] = file_diff
    else:
        code_review_result["file_content"] = file_content
    ui_print(f"Code review result: {code_review_result}")
    json_data = json.dumps(code_review_result, indent=4, ensure_ascii=False)
    json_name = get_json_name(scan_scope, project, branch, commit_id, file_name)
    S3.put_object(
        Bucket=BUCKET_NAME,
        Key=json_name,
        Body=json_data,
        ContentType="application/json",
    )
    
    project_branch_file = str(project + "_" + branch + "_" + file_name)
    version = 0
    print("start deal with " + project_branch_file)
    request_item_v0 = get_request_item(
        project_branch_file,
        0,
        0,
        datetime.now(),
        review_id,
        commit_id,
        review_score,
        review_result
    )
    insert_dynamodb_v0(request_item_v0)
    print("try to insert v0.")
    
    file_review = get_file_status_recode(project_branch_file, version)
    latest = file_review["Item"]["latest"] + 1
    print("latest version is " + str(latest))
    
    # insert the latest record
    request_item_vn = get_request_item(
        project_branch_file,
        latest,
        None,
        datetime.now(),
        review_id,
        commit_id,
        review_score,
        review_result
    )
    print("request_item_vn for " + project_branch_file)
    
    insert_dynamodb(request_item_vn)
    print("insert file " + project_branch_file + " , v" + str(latest))
    
    update_item_v0 = get_request_item(
        project_branch_file,
        0,
        latest,
        datetime.now(),
        review_id,
        commit_id,
        review_score,
        review_result
    )
    if commit_id != "00000000":
        update_dynamodb_version(update_item_v0)
    else:
        update_dynamodb_only_version(update_item_v0)
    print("update file " + project_branch_file + " , v0 latest!")

    update_dynamodb_done_file(review_id)


def process_failed_reply(msg_body, review_id):
    msg_body = increment_field(msg_body, "failed_times")
    failed_times = get_field(msg_body, "failed_times")
    ui_print(f"failed_times: {failed_times}")
    if failed_times > MAX_FAILED_TIMES:
        update_dynamodb_file_num(review_id)
    else:
        SQS.send_message(
            QueueUrl=TASK_SQS_URL,
            MessageBody=json.dumps(msg_body, indent=4, ensure_ascii=False),
            DelaySeconds=2**failed_times * 10,
        )


def handle_reply(
    msg_body,
    reply,
    output_tokens,
    scan_scope,
    commit_id,
    file_name,
    file_content,
    file_diff,
    project,
    branch,
    review_id,
):
    if reply != BEDROCK_ERROR_MSG:
        return process_successful_reply(
            reply,
            output_tokens,
            scan_scope,
            commit_id,
            file_name,
            file_content,
            file_diff,
            project,
            branch,
            review_id,
        )
    else:
        return process_failed_reply(msg_body, review_id)


def handle_failure(msg_body, review_id):
    msg_body = increment_field(msg_body, "failed_times")
    failed_times = get_field(msg_body, "failed_times")
    ui_print(f"failed_times: {failed_times}")
    if failed_times > MAX_FAILED_TIMES:
        update_dynamodb_file_num(review_id)
    else:
        SQS.send_message(
            QueueUrl=TASK_SQS_URL,
            MessageBody=json.dumps(msg_body, indent=4, ensure_ascii=False),
            DelaySeconds=2**failed_times * 10,
        )


def process_record_review(record):
    try:
        msg_body = json.loads(record["body"].encode("utf-8"))
        ui_print(f"Msg body from sqs: {msg_body}")
        # 提取消息体中的内容
        review_id, project, branch, commit_id, file_list, file_name, file_content, scan_scope = (
            extract_message_details(msg_body)
        )
        file_diff = extract_file_diff(msg_body, scan_scope)
        full_prompt = get_full_prompt(scan_scope, file_content, file_diff)
        SQS.delete_message(QueueUrl=TASK_SQS_URL, ReceiptHandle=record["receiptHandle"])
        reply, output_tokens = invoke_bedrock(full_prompt)
        return handle_reply(
            msg_body,
            reply,
            output_tokens,
            scan_scope,
            commit_id,
            file_name,
            file_content,
            file_diff,
            project,
            branch,
            review_id,
        )
    except Exception as e:
        ui_print(f"Error processing message: {str(e)}")
        return handle_failure(msg_body, review_id)


def get_record_type(record):
    try:
        msg_body = json.loads(record["body"].encode("utf-8"))
        ui_print(f"Msg body from sqs: {msg_body}")
        return msg_body["msg_type"]
    except Exception as e:
        ui_print(f"Error processing message: {str(e)}")
        return ""


def generate_code_review_all_html(json_data):
    try:
        env = Environment(autoescape=select_autoescape())

        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Code Review</title>
            <meta charset="UTF-8">
            <style>
                body {
                    background-color: #000000;
                    color: #ffffff;
                    font-family: monospace;
                    padding: 20px;
                }
                pre {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    white-space: pre-wrap;
                    border: 1px solid #444444;
                }
                .container {
                    max-width: 800px;
                    margin: 0 auto;
                }
                .commit-info {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                    display: flex;
                    align-items: center;
                }
                .commit-info img {
                    width: 30px;
                    height: 30px;
                    margin-right: 10px;
                    filter: invert(100%);
                }
                .header {
                    text-align: center;
                    margin-bottom: 20px;
                }
                .header img {
                    width: 175px;
                    height: 32px;
                }
                .collapsible {
                    background-color: #222222;
                    color: white;
                    cursor: pointer;
                    padding: 10px;
                    width: 100%;
                    border: none;
                    text-align: left;
                    outline: none;
                    font-size: 15px;
                }
                .collapsible:after {
                    content: '\\25BC'; /* Unicode character for "plus" sign (+) */
                    font-size: 13px;
                    color: white;
                    float: right;
                    margin-left: 5px;
                }
                .active:after {
                    content: "\\25B2"; /* Unicode character for "minus" sign (-) */
                }
                .content {
                    padding: 0 18px;
                    max-height: 0;
                    overflow: hidden;
                    transition: max-height 0.2s ease-out;
                    background-color: #21252b;
                }
                .code-block {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    font-family: 'Courier New', Courier, monospace;
                    font-size: 14px;
                    line-height: 1.5;
                    overflow-x: auto;
                }
                .code-block .line {
                    display: flex;
                }
                .code-block .line-number {
                    color: #5c6370;
                    padding-right: 10px;
                    text-align: right;
                    user-select: none;
                }
                .code-block .code {
                    flex: 1;
                }
                .code-block .code .highlight {
                    color: #e06c75;
                }
                .code-block .code .highlight-string {
                    color: #98c379;
                }
                .code-block .code .highlight-number {
                    color: #d19a66;
                }
                .code-block .code .highlight-keyword {
                    color: #c678dd;
                    font-weight: bold;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                </div>
                {% for commit_id in unique_commit_ids %}
                <h1>Repo Latest Commit: {{ commit_id }}</h1>
                {% for item in commit_data[commit_id] %}
                <h2>File Name: {{ item['file_name'] }}</h2>
                <button class="collapsible">File Content</button>
                <div class="content">
                    <div class="code-block">
                        {% for line in item['highlighted_content'].splitlines() %}
                        <div class="line">
                            <span class="line-number">{{ loop.index }}</span>
                            <span class="code">{{ line|replace('<span class="highlight">', '<span class="highlight highlight-keyword">')|replace('<span class="highlight-string">', '<span class="highlight highlight-string">')|replace('<span class="highlight-number">', '<span class="highlight highlight-number">')|safe }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
                <button class="collapsible">Review Result</button>
                <div class="content">
                    <pre>{{ item['review_result']|e }}</pre>
                </div>
                {% endfor %}
                {% endfor %}
            </div>
            <script>
                var coll = document.getElementsByClassName("collapsible");
                var i;

                for (i = 0; i < coll.length; i++) {
                    coll[i].addEventListener("click", function() {
                        this.classList.toggle("active");
                        var content = this.nextElementSibling;
                        if (content.style.maxHeight) {
                            content.style.maxHeight = null;
                        } else {
                            content.style.maxHeight = content.scrollHeight + "px";
                        }
                    });
                }
            </script>
        </body>
        </html>
        """

        # Get unique commit IDs
        unique_commit_ids = set(item["commit_id"] for item in json_data)

        # Store highlighted content for each commit_id
        commit_data = {}
        for commit_id in unique_commit_ids:
            commit_data[commit_id] = []
            for item in json_data:
                if item["commit_id"] == commit_id:
                    try:
                        lexer = get_lexer_for_filename(item["file_name"])
                        formatter = HtmlFormatter(linenos=False, cssclass="source")
                        highlighted_content = highlight(
                            item["file_content"], lexer, formatter
                        )
                    except ValueError:
                        highlighted_content = item["file_content"]

                    commit_data[commit_id].append(
                        {
                            "file_name": item["file_name"],
                            "highlighted_content": highlighted_content,
                            "review_result": item["review_result"],
                        }
                    )

        template = env.from_string(html)
        return template.render(
            unique_commit_ids=unique_commit_ids,
            commit_data=commit_data,
        )
    except Exception as e:
        ui_print(f"An error occurred: {e}")
    return HTML_GEN_ERROR


def generate_code_review_diff_html(json_data):
    try:
        env = Environment(autoescape=select_autoescape())

        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Code Review</title>
            <meta charset="UTF-8">
            <style>
                body {
                    background-color: #000000;
                    color: #ffffff;
                    font-family: monospace;
                    padding: 20px;
                }
                pre {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    white-space: pre-wrap;
                    border: 1px solid #444444;
                }
                .container {
                    max-width: 800px;
                    margin: 0 auto;
                }
                .commit-info {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                    display: flex;
                    align-items: center;
                }
                .commit-info img {
                    width: 30px;
                    height: 30px;
                    margin-right: 10px;
                    filter: invert(100%);
                }
                .header {
                    text-align: center;
                    margin-bottom: 20px;
                }
                .header img {
                    width: 175px;
                    height: 32px;
                }
                .collapsible {
                    background-color: #222222;
                    color: white;
                    cursor: pointer;
                    padding: 10px;
                    width: 100%;
                    border: none;
                    text-align: left;
                    outline: none;
                    font-size: 15px;
                }
                .collapsible:after {
                    content: '\\25BC'; /* Unicode character for "plus" sign (+) */
                    font-size: 13px;
                    color: white;
                    float: right;
                    margin-left: 5px;
                }
                .active:after {
                    content: "\\25B2"; /* Unicode character for "minus" sign (-) */
                }
                .content {
                    padding: 0 18px;
                    max-height: 0;
                    overflow: hidden;
                    transition: max-height 0.2s ease-out;
                    background-color: #21252b;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                </div>
                {% for commit_id in unique_commit_ids %}
                <h1>Commit: {{ commit_id }}</h1>
                {% endfor %}
                {% for commit_id in unique_commit_ids %}
                {% for item in json_data %}
                {% if item['commit_id'] == commit_id %}
                <h2>File Name: {{ item['file_name'] }}</h2>
                <button class="collapsible">Diff</button>
                <div class="content">
                    <pre>{{ item['diff']|e }}</pre>
                </div>
                <button class="collapsible">Review Result</button>
                <div class="content">
                    <pre>{{ item['review_result']|e }}</pre>
                </div>
                {% endif %}
                {% endfor %}
                {% endfor %}
            </div>
            <script>
                var coll = document.getElementsByClassName("collapsible");
                var i;

                for (i = 0; i < coll.length; i++) {
                    coll[i].addEventListener("click", function() {
                        this.classList.toggle("active");
                        var content = this.nextElementSibling;
                        if (content.style.maxHeight) {
                            content.style.maxHeight = null;
                        } else {
                            content.style.maxHeight = content.scrollHeight + "px";
                        }
                    });
                }
            </script>
        </body>
        </html>
        """

        # Get unique commit IDs
        unique_commit_ids = set(item["commit_id"] for item in json_data)

        template = env.from_string(html)
        rendered_html = template.render(
            json_data=json_data, unique_commit_ids=unique_commit_ids
        )

        # with open("code_review.html", "w", encoding="utf-8") as f:
        #     f.write(rendered_html)
        return rendered_html
    except Exception as e:
        ui_print(f"An error occurred: {e}")
    return HTML_GEN_ERROR


def generate_summary_html(json_data):
    try:
        env = Environment(autoescape=select_autoescape())

        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Code Review</title>
            <meta charset="UTF-8">
            <style>
                body {
                    background-color: #000000;
                    color: #ffffff;
                    font-family: monospace;
                    padding: 20px;
                }
                pre {
                    background-color: #222222;
                    padding: 10px;
                    border-radius: 5px;
                    white-space: pre-wrap;
                    border: 1px solid #444444;
                }
                .container {
                    max-width: 800px;
                    margin: 0 auto;
                }
                .header {
                    text-align: center;
                    margin-bottom: 20px;
                }
                .header img {
                    width: 175px;
                    height: 32px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                </div>
                <h1>Review ID: {{ json_data['review_id'] }}</h1>
                <h2>Review Summary</h2>
                <pre>{{ json_data['review_summary'] }}</pre>
            </div>
        </body>
        </html>
        """

        template = env.from_string(html)
        return template.render(json_data=json_data)
    except Exception as e:
        ui_print(f"An error occurred: {e}")
    return HTML_GEN_ERROR


def generate_code_review_html(json_data, scan_scope):
    if scan_scope == ALL_SCAN_SCOPE:
        return generate_code_review_all_html(json_data)
    return generate_code_review_diff_html(json_data)


def gen_prefix(commit_id, scan_scope, project, branch):
    return scan_scope + "/" + project + "/" + branch + "/" + commit_id + "/"


def gen_merge_file_key(commit_id, scan_scope, project, branch):
    return gen_prefix(commit_id, scan_scope, project, branch) + HTML_POSTFIX


def gen_reveiw_summary_key(commit_id, scan_scope, project, branch):
    return gen_prefix(commit_id, scan_scope, project, branch) + SUMMARY_HTML_POSTFIX


def handle_send_message(message, sqs_url=TASK_SQS_URL):
    try:
        SQS.send_message(QueueUrl=sqs_url, MessageBody=message)
        return True
    except Exception as e:
        print("An unexpected error occurred:", e)
        return False


def merge_json_files_concurrently(
    scan_scope,
    prefix,
    merged_file_key,
    bucket=BUCKET_NAME,
    max_workers=40,
):
    try:
        # 获取所有以.json结尾的文件的元数据
        response = S3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        json_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".json")
        ]

        files_num = len(json_files)
        if not json_files:
            ui_print("No .json files found.")
            return NO_FILE_NEED_REVIEW

        # 定义并发下载文件内容的函数
        def download_file(file_key):
            response = S3.get_object(Bucket=bucket, Key=file_key)
            return json.loads(response["Body"].read())

        # 使用线程池并发下载文件内容
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(download_file, file_key) for file_key in json_files
            ]
            all_data = [future.result() for future in as_completed(futures)]

        # 将合并后的数据转换为JSON格式的字符串
        # print(all_data)
        merged_json_string = json.dumps(all_data, indent=4, ensure_ascii=False)
        json_data = json.loads(merged_json_string)

        html_content = generate_code_review_html(json_data, scan_scope)
        # metadata = {'Content-Type': 'text/html'}
        S3.put_object(
            Bucket=bucket,
            Key=merged_file_key,
            Body=html_content,
            ContentType="Content-Type: text/html",
        )
        ui_print(
            f"Merged JSON file '{merged_file_key}' has been uploaded to bucket '{BUCKET_NAME}'."
        )
        return json_data
    except (NoCredentialsError, ClientError) as e:
        ui_print(f"An error occurred: {e}")
        return {}


def send_review_summary_msg(
    json_data, review_id, project, branch, commit_id, file_list, scan_scope
):
    prompt = gen_review_summary_prompt(json_data)
    prompt_key = "review-summary-prompt-" + review_id + ".txt"
    S3.put_object(
        Bucket=BUCKET_NAME,
        Key="review-summary-prompt-" + review_id + ".txt",
        Body=prompt.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )
    item = {
        "review_id": review_id,
        "project": project,
        "branch": branch,
        "file_name": "review-summary",
        "file_content": "",
        "prompt_key": prompt_key,
        "commit_id": commit_id,
        "file_list": file_list,
        "scan_scope": scan_scope,
        "msg_type": REVIEW_SUMMARY,
    }
    return handle_send_message(json.dumps(item, indent=4, ensure_ascii=False))


def get_scores(json_data):
    min_score = 101
    max_score = -1
    avg_score = 101
    try:
        if not json_data:
            raise ValueError("The input JSON data is empty.")
        sum_score = 0
        file_num = len(json_data)

        for item in json_data:
            if "review_score" not in item:
                raise KeyError("Missing 'review_score' key in one of the items.")
            if not isinstance(item["review_score"], (int, float)):
                raise ValueError(
                    f"Non-numeric value found for 'review_score': {item['review_score']}"
                )
            min_score = min(min_score, item["review_score"])
            max_score = max(max_score, item["review_score"])
            sum_score += item["review_score"]

        if file_num != 0:
            avg_score = int(sum_score / file_num)
    except Exception as e:
        ui_print(f"Error: {e}")
    return {"min_score": min_score, "max_score": max_score, "avg_score": avg_score}


def gen_review_summary_msg(record):
    try:
        msg_body = json.loads(record["body"].encode("utf-8"))
        ui_print(f"Msg body from sqs: {msg_body}")
        # 提取消息体中的内容
        review_id, project, branch, commit_id, file_list, file_name, file_content, scan_scope = (
            extract_message_details(msg_body)
        )
        responses = query_dynamodb_by_review_id(review_id)
        for request_item in responses:
            ui_print(request_item)
            if can_merge_review_result(request_item) is True:
                file_review_html_key = gen_merge_file_key(
                    commit_id, scan_scope, project, branch
                )
                json_data = merge_json_files_concurrently(
                    scan_scope,
                    prefix=gen_prefix(commit_id, scan_scope, project, branch),
                    merged_file_key=file_review_html_key,
                )
                scores = get_scores(json_data)
                update_dynamodb_file_review_html_key(
                    review_id, file_review_html_key, scores
                )
                if scan_scope == ALL_SCAN_SCOPE:
                    status = send_review_summary_msg(
                        json_data, review_id, project, branch, commit_id, file_list, scan_scope
                    )
                    if status is False:
                        update_dynamodb_stask_status(review_id)

                else:
                    update_dynamodb_stask_status(review_id)

    except Exception as e:
        ui_print(f"Error processing message: {str(e)}")


def read_s3_object(prompt_key):

    try:
        # 获取对象
        response = S3.get_object(Bucket=BUCKET_NAME, Key=prompt_key)

        # 读取对象内容
        content = response["Body"].read().decode("utf-8")

        return str(content)
    except Exception as e:
        print(f"Error reading object {prompt_key} from bucket {BUCKET_NAME}: {e}")
        return ""


def process_record_summary_review(record):
    try:
        msg_body = json.loads(record["body"].encode("utf-8"))
        ui_print(f"Msg body from sqs: {msg_body}")
        # 提取消息体中的内容
        review_id, project, branch, commit_id, file_list, file_name, file_content, scan_scope = (
            extract_message_details(msg_body)
        )
        prompt_key = msg_body["prompt_key"]
        full_prompt = read_s3_object(prompt_key)
        SQS.delete_message(QueueUrl=TASK_SQS_URL, ReceiptHandle=record["receiptHandle"])
        print(full_prompt)
        reply, token_num = invoke_bedrock(full_prompt)
        ui_print(f"token_num: {token_num}")
        if reply != BEDROCK_ERROR_MSG:
            code_review_result = {}
            code_review_result["review_id"] = review_id
            code_review_result["review_summary"] = reply
            json_data = code_review_result
            review_summary_html_key = gen_reveiw_summary_key(
                commit_id, scan_scope, project, branch
            )
            html_content = generate_summary_html(json_data)
            update_dynamodb_review_summary_html_key(review_id, review_summary_html_key)
            S3.put_object(
                Bucket=BUCKET_NAME,
                Key=review_summary_html_key,
                Body=html_content,
                ContentType="Content-Type: text/html",
            )
            update_dynamodb_stask_status(review_id)
            # S3.put_object(
            #     Bucket=BUCKET_NAME,
            #     Key=review_summary_html_key,
            #     Body=json_data,
            #     ContentType="application/json",
            # )
        else:
            msg_body = increment_field(msg_body, "failed_times")
            failed_times = get_field(msg_body, "failed_times")
            ui_print(f"failed_times: {failed_times}")
            if failed_times <= max(MAX_FAILED_TIMES, 6):
                SQS.send_message(
                    QueueUrl=TASK_SQS_URL,
                    MessageBody=json.dumps(msg_body, indent=4, ensure_ascii=False),
                    DelaySeconds=2**failed_times * 10,
                )
            else:
                update_dynamodb_stask_status(review_id)

    except Exception as e:
        ui_print(f"Error processing message: {str(e)}")
        msg_body = increment_field(msg_body, "failed_times")
        failed_times = get_field(msg_body, "failed_times")
        ui_print(f"failed_times: {failed_times}")
        if failed_times <= max(MAX_FAILED_TIMES, 6):
            SQS.send_message(
                QueueUrl=TASK_SQS_URL,
                MessageBody=json.dumps(msg_body, indent=4, ensure_ascii=False),
                DelaySeconds=2**failed_times * 10,
            )
        else:
            update_dynamodb_stask_status(review_id)


def lambda_handler(event, context):
    if event:
        record_size = len(event["Records"])
        ui_print(f"Record size: {record_size}")
        for record in event["Records"]:
            msg_type = get_record_type(record)
            msg_body = json.loads(record["body"].encode("utf-8"))
            # 提取消息体中的内容
            review_id, project, branch, commit_id, file_list, file_name, file_content, scan_scope = (
                extract_message_details(msg_body)
            )
            if msg_type == File_REVIEW:
                process_record_review(record)
                if file_list == []:
                    gen_review_summary_msg(record)
                else:
                    update_dynamodb_stask_status(review_id)
            else:
                process_record_summary_review(record)

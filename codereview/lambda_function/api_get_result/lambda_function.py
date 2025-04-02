import json
import boto3
import os
from botocore.config import Config
import json
import logging
from datetime import datetime, timedelta


DYNAMODB = boto3.client("dynamodb")
REPO_CODE_REVIEW_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_TABLE_NAME")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LAMBDA_LOG_BUCKET_NAME = os.getenv("LAMBDA_LOG_BUCKET_NAME")
EXPIRES_IN = int(os.getenv("EXPIRES_IN", "36000"))
HTML_POSTFIX = "merged-code-review-result.html"
S3 = boto3.client("s3")
NO_FILE_NEED_REVIEW = "No file need review"
PRESIGNED_URL_GEN_ERROR = "An error occurred in generating presigned URL"
HTML_GEN_ERROR = "An error occurred in generating html"
ALL_SCAN_SCOPE = "ALL"
COMPLETED_STATUS = "Completed"
PROGRESS_STATUS = "InProgress"
PROGRESSLLM_STATUS = "InProgress LLM"


GSI_INDEX_NAMES = {
    "PROJECT_INDEX": "project_created_at_gsi",
    "SCAN_SCOPE_INDEX": "scan_scope_created_at_gsi",
    "COMMIT_ID_INDEX": "commit_id_created_at_gsi",
    "REPO_URL_INDEX": "repo_url_created_at_gsi",
    "BRANCH_INDEX": "branch_created_at_gsi",
}

RESPONSE_HEADERS = {
    "Access-Control-Allow-Origin": "*",  # 允许来自任何源的请求
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent",  # 允许的请求头
    "Access-Control-Allow-Methods": "OPTIONS,GET,PUT,POST,DELETE",  # 允许的 HTTP 方法
}

logging.basicConfig(
    force=True,
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ui_print(log, lambda_name="lambda_api_get_result"):
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


def query_task_status(review_id):
    """
    查询指定review_id的任务状态。

    Parameters:
    review_id (str): 要查询的review的ID。

    Returns:
    dict: 查询到的DynamoDB响应，或在发生异常时返回错误信息。
    """
    query_key_condition = {
        "KeyConditionExpression": "#review_id = :key_value",
        "ExpressionAttributeNames": {"#review_id": "review_id"},
        "ExpressionAttributeValues": {
            ":key_value": {"S": review_id},
        },
    }

    try:
        # 执行查询
        response = DYNAMODB.query(
            TableName=REPO_CODE_REVIEW_TABLE_NAME, **query_key_condition
        )
        return response
    except Exception as e:
        # 打印错误信息并返回None或自定义错误消息
        print(f"An error occurred: {str(e)}")
        return None


def count_next_month(
    index_name,
    partition_key,
    partition_value,
    query_params,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
):
    year = int(partition_value.split('-')[0])
    print("year is " + str(year))
    month = int(partition_value.split('-')[1])
    print("month is " + str(month))
    
    count = 0
    iteration = 0
    # Initialize filter expression components
    expression_attribute_names = {}
    expression_attribute_values = {}
    
    while iteration < 2:
        print("iteration is " + str(iteration))
        print("month is " + str(month))
        if month > 1:
            month = month - 1
            partition_value = str(year) + "-0" + str(month)
            print("partition_value is " + partition_value)
            expression_attribute_values[":value"] = {"S": partition_value}
            print("expression_attribute_values update.")
            query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        else:
            month = 12
            year = year - 1
            partition_value = str(year) + "-0" + str(month)
            expression_attribute_values[":value"] = {"S": partition_value}
            query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
        print("query parameters is " + str(query_params))
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        print("response count is " + str(response.get("Count", 0)))
        count = count + int(response.get("Count", 0))
        print("iteration is " + str(iteration) + ", count is "+ str(count))
        iteration = iteration + 1
    return count

def count_items_in_dynamodb(
    index_name,
    partition_key,
    partition_value,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
):
    # 初始化查询参数
    query_params = {
        "KeyConditionExpression": f"#{partition_key} = :value",
        "ExpressionAttributeNames": {f"#{partition_key}": partition_key},
        "ExpressionAttributeValues": {":value": {"S": partition_value}},
        "Select": "COUNT",  # 设置查询为计数模式
    }

    # 初始化过滤表达式组件
    expression_attribute_names = {}
    expression_attribute_values = {}
    
    query_params["FilterExpression"] = "NOT commit_id = :commit_id"
    expression_attribute_values[":commit_id"] = {"S": "00000000"}
    query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
    try:
        # 执行查询
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        count = response.get("Count", 0)
        print("partition_key is " + partition_key)
        print("partition_value is " + partition_value)
        print("first month count is " + str(count))
        count = count + count_next_month(index_name, partition_key, partition_value, query_params)
        return count
        
    except Exception as e:
        ui_print(f"An error occurred: {str(e)}")
        return None


def query_with_count(
    index_name,
    partition_key,
    partition_value,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
    filter_parameters={},
):

    # 初始化查询参数
    query_params = {
        "KeyConditionExpression": f"#{partition_key} = :value",
        "ExpressionAttributeNames": {f"#{partition_key}": partition_key},
        "ExpressionAttributeValues": {":value": {"S": partition_value}},
        "Select": "COUNT",  # 设置查询为计数模式
    }

    # 初始化过滤表达式组件
    filter_expressions = []
    expression_attribute_names = {}
    expression_attribute_values = {}

    # 根据非空值构建过滤表达式，并确保它们不与分区键匹配
    for key in ["commit_id", "project", "scan_scope", "repo_url", "branch"]:
        if key != partition_key and filter_parameters.get(key, ""):
            filter_expressions.append(f"#{key} = :{key}")
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":{key}"] = {"S": filter_parameters[key]}

    # 如果存在过滤表达式，将它们添加到查询参数中
    if filter_expressions:
        query_params["FilterExpression"] = " AND ".join(filter_expressions)
        query_params["ExpressionAttributeNames"].update(expression_attribute_names)
        query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        if "commit_id" not in query_params["FilterExpression"] and partition_key != "commit_id":
            query_params["FilterExpression"] = query_params["FilterExpression"] + " AND NOT commit_id = :commit_id"
            expression_attribute_values[":commit_id"] = {"S": "00000000"}
    else:
        if partition_key != "commit_id":
            query_params["FilterExpression"] = "NOT commit_id = :commit_id"
            expression_attribute_values[":commit_id"] = {"S": "00000000"}
    query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
    try:
        # 执行查询
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        # 返回符合条件的记录数量
        return response.get("Count", 0)
    except Exception as e:
        ui_print(f"An error occurred: {str(e)}")
        return None


def query_with_pagination(
    index_name,
    partition_key,
    partition_value,
    page_index,
    page_size,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
    filter_parameters={},
):

    # 初始化查询参数
    query_params = {
        "KeyConditionExpression": f"#{partition_key} = :value",
        "ExpressionAttributeNames": {f"#{partition_key}": partition_key},
        "ExpressionAttributeValues": {":value": {"S": partition_value}},
        "ScanIndexForward": False,  # 倒序排列
    }
    # Initialize filter expression components
    filter_expressions = []
    expression_attribute_names = {}
    expression_attribute_values = {}

    # Build filter expression based on non-empty values and not matching the partition key
    for key in ["commit_id", "project", "scan_scope", "repo_url", "branch"]:
        if key != partition_key and filter_parameters.get(key, ""):
            filter_expressions.append(f"#{key} = :{key}")
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":{key}"] = {"S": filter_parameters[key]}

    # If there are filter expressions, add them to the query parameters
    if filter_expressions:
        query_params["FilterExpression"] = " AND ".join(filter_expressions)
        query_params["ExpressionAttributeNames"].update(expression_attribute_names)
        query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        if "commit_id" not in query_params["FilterExpression"] and partition_key != "commit_id":
            query_params["FilterExpression"] = query_params["FilterExpression"] + " AND NOT commit_id = :commit_id"
            expression_attribute_values[":commit_id"] = {"S": "00000000"}
    else:
        if partition_key != "commit_id":
            query_params["FilterExpression"] = "NOT commit_id = :commit_id"
            expression_attribute_values[":commit_id"] = {"S": "00000000"}
    query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
    index_count = 0
    items_count = 0
    try:
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        # 处理查询结果
        ui_print(response)
        
        if page_index * page_size <= len(response['Items']):
            response['Items'] = response['Items'][(page_index-1) * page_size : page_index * page_size]
        else:
            response['Items'] = response['Items'][(page_index-1) * page_size : ]
        return response
            
    except Exception as e:
        ui_print(f"An error occurred: {str(e)}")
        return None


def full_scan_next_month(
    index_name,
    partition_key,
    partition_value,
    query_params,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
):
    year = int(partition_value.split('-')[0])
    month = int(partition_value.split('-')[1])
    
    items = []
    iteration = 0
    
    # Initialize filter expression components
    expression_attribute_names = {}
    expression_attribute_values = {}
    
    while iteration < 2:
        if month > 1:
            month = month - 1
            partition_value = str(year) + "-0" + str(month)
            expression_attribute_values[":value"] = {"S": partition_value}
            query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        else:
            month = 12
            year = year - 1
            partition_value = str(year) + "-0" + str(month)
            expression_attribute_values[":value"] = {"S": partition_value}
            query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
        print("next month query parameters is " + str(query_params))
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        items.extend(response['Items'])
        iteration = iteration + 1
    return items
    

def full_table_scan(
    index_name,
    partition_key,
    partition_value,
    page_index,
    page_size,
    table_name=REPO_CODE_REVIEW_TABLE_NAME,
):
    # 初始化查询参数
    query_params = {
        "KeyConditionExpression": f"#{partition_key} = :value",
        "ExpressionAttributeNames": {f"#{partition_key}": partition_key},
        "ExpressionAttributeValues": {":value": {"S": partition_value}},
        "ScanIndexForward": False,  # 倒序排列
    }
    # Initialize filter expression components
    expression_attribute_names = {}
    expression_attribute_values = {}

    query_params["FilterExpression"] = "NOT commit_id = :commit_id"
    expression_attribute_values[":commit_id"] = {"S": "00000000"}
    query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
    index_count = 0
    items = []
    print("query parameters is " + str(query_params))
    try:
        response = DYNAMODB.query(
            TableName=table_name, IndexName=index_name, **query_params
        )
        # 处理查询结果
        ui_print(response)
        print("query first response is " + str(response))
        items.extend(response['Items'])
        print("start next month query")
        items.extend(full_scan_next_month(index_name, partition_key, partition_value, query_params))
        
        if page_index * page_size <= len(items):
            items = items[(page_index-1) * page_size : page_index * page_size]
        else:
            items = items[(page_index-1) * page_size : ]
        return items
        
    except Exception as e:
        ui_print(f"An error occurred: {str(e)}")
        return None


def get_presigned_url(file_review_html_key, review_summary_html_key):
    urls = []
    try:
        presigned_url_file_review = ""
        presigned_url_review_summary = ""
        if file_review_html_key:
            presigned_url_file_review = S3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET_NAME, "Key": file_review_html_key},
                ExpiresIn=EXPIRES_IN,
            )  # URL有效期为1小时
        if review_summary_html_key:
            presigned_url_review_summary = S3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET_NAME, "Key": review_summary_html_key},
                ExpiresIn=EXPIRES_IN,
            )  # URL有效期为1小时
        if presigned_url_file_review.startswith("https"):
            urls.append(presigned_url_file_review)
        if presigned_url_review_summary.startswith("https"):
            urls.append(presigned_url_review_summary)
    except Exception as e:
        ui_print(f"An error occurred: {str(e)}")
    return urls


def return_review_result(status, review_result=[], message=""):
    current_time = datetime.now()

    response = {
        "status": status,
        "message": message,
        "timestamp": str(current_time),
        "data": {
            "review_result": review_result,
        },
    }
    res = {"statusCode": 200, "headers": RESPONSE_HEADERS, "body": json.dumps(response)}
    ui_print(res)
    return res


def return_review_records(status, total_records=0, data=[], message=""):
    current_time = datetime.now()
    response = {
        "status": status,
        "message": message,
        "timestamp": str(current_time),
        "total_records": total_records,
        "data": data,
    }
    res = {"statusCode": 200, "headers": RESPONSE_HEADERS, "body": json.dumps(response)}
    ui_print(res)
    return res


def get_review_result(event):
    try:
        body = json.loads(event["body"])
        ui_print(body)
        review_id = body["review_id"]
        response = query_task_status(review_id)
        size = len(response["Items"])
        if size == 0:
            return return_review_result(
                status="failure",
                message=f"There is no code review result about this query. Please ensure you have submit this code reveiw task .",
            )
        ui_print(f"len of response: {size}")
        request_item = response["Items"][0]
        ui_print(request_item)
        file_done = int(request_item.get("file_done", {}).get("N"))
        file_num = int(request_item.get("file_num", {}).get("N"))
        task_status = request_item.get("task_status", {}).get("S")
        review_summary_html_key = request_item.get("review_summary_html_key", {}).get(
            "S"
        )
        file_review_html_key = request_item.get("file_review_html_key", {}).get("S")
        if task_status == PROGRESS_STATUS:
            return return_review_result(
                status="failure",
                message=f"The task is in init progress.",
            )
        elif task_status == PROGRESSLLM_STATUS:
            return return_review_result(
                status="failure",
                message=f"The task is in progress,Done {file_done},ALL {file_num}",
            )
        elif task_status == COMPLETED_STATUS:
            presigned_urls = get_presigned_url(
                file_review_html_key, review_summary_html_key
            )
            ui_print(presigned_urls)
            if len(presigned_urls) >= 1:
                return return_review_result(
                    status="success",
                    review_result=presigned_urls,
                )
            else:
                return return_review_result(
                    status="failure", message="gen presigned url error"
                )
        else:
            return return_review_result(status="failure", message="not known status")
    except Exception as e:
        ui_print(str(e))
        return return_review_result(status="failure", message=str(e))


def get_review_records(event):
    try:
        body = json.loads(event["body"])
        ui_print(body)  #

        # Validate page index and size
        page_index = body.get("page_index", 0)
        page_size = body.get("page_size", 0)
        if page_index <= 0 or page_size <= 0:
            return return_review_records(
                status="failure",
                message="page_index and page_size should be larger than 0.",
            )

        # Extract parameters
        parameters = {
            "commit_id": body.get("commit_id", ""),
            "project": body.get("project", ""),
            "scan_scope": body.get("scan_scope", ""),
            "repo_url": body.get("repo_url", ""),
            "branch": body.get("branch", ""),
        }

        # Query based on the first non-empty parameter
        for key, value in parameters.items():
            print("start query items!")
            if value:
                index_name = f"{key.upper()}_INDEX"
                count = query_with_count(
                    GSI_INDEX_NAMES[index_name],
                    key,
                    value,
                    filter_parameters=parameters,
                )
                ui_print(f"total_records :{count}")
                if count > 0:
                    last_page_index = max(page_index - 1, 0)
                    if count <= last_page_index * page_size:
                        return_review_records(
                            status="failure",
                            message="page_index and page_size should be valid.",
                        )
                    response = query_with_pagination(
                        GSI_INDEX_NAMES[index_name],
                        key,
                        value,
                        page_index,
                        page_size,
                        filter_parameters=parameters,
                    )
                    print("Finish query items!")

                    if response and response.get("Items"):
                        return return_review_records(
                            status="success",
                            total_records=count,
                            data=response["Items"],
                        )
                    break
                elif count == 0:
                    return return_review_records(
                        status="success",
                        total_records=count,
                        data=[],
                    )
                    break
                    
        count = count_items_in_dynamodb('year_month-created_at-index', 'year_month', datetime.now().strftime('%Y-%m'))
        if count > 0:
            last_page_index = max(page_index - 1, 0)
            if count <= last_page_index * page_size:
                return_review_records(
                    status="failure",
                    message="page_index and page_size should be valid.",
                )
            print("start full scan!")
            page_items = full_table_scan(
                'year_month-created_at-index',
                'year_month',
                datetime.now().strftime('%Y-%m'),
                page_index=page_index,
                page_size=page_size,
            )
            return return_review_records(
                status="success",
                total_records=count,
                data=page_items,
            )

        # If no valid response or parameters were empty
        return return_review_records(
            status="failure",
            message="No valid code review records found. Please check the query parameters.",
        )

    except Exception as e:
        ui_print(str(e))
        return return_review_records(status="failure", message=str(e))


def lambda_handler(event, context):
    ui_print(event)
    path = event["path"]
    if path == "/getReviewResult":
        return get_review_result(event)
    elif path == "/getReviewRecords":
        return get_review_records(event)
    else:
        res = {
            "statusCode": 404,
            "headers": RESPONSE_HEADERS,
            "body": json.dumps("API endpoint not found"),
        }
        ui_print(res)
        return res

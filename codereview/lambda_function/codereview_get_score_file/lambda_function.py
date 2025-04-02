import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timedelta
import json
import logging
import os
from botocore.config import Config
import decimal

LAMBDA_LOG_BUCKET_NAME = os.getenv("LAMBDA_LOG_BUCKET_NAME")
client_config = Config(max_pool_connections=50)
S3 = boto3.client("s3", config=client_config)
DYNAMODB = boto3.client("dynamodb")

# 创建 DynamoDB 客户端
dynamodb = boto3.resource('dynamodb')
REPO_CODE_REVIEW_SCORE_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_SCORE_TABLE_NAME")
REPO_CODE_REVIEW_SCORE_TABLE = dynamodb.Table(REPO_CODE_REVIEW_SCORE_TABLE_NAME)

RESPONSE_HEADERS = {
    "Access-Control-Allow-Origin": "*",  # 允许来自任何源的请求
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent",  # 允许的请求头
    "Access-Control-Allow-Methods": "OPTIONS,GET,PUT,POST,DELETE",  # 允许的 HTTP 方法
}

def ui_print(log, lambda_name="codereview_get_score_file"):
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
    
def decimal_serializer(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)
    
    
def return_score_files(status, data={}, message=""):
    current_time = datetime.now()
    response = {
        "status": status,
        "message": message,
        "timestamp": str(current_time),
        "data": data,
    }
    res = {"statusCode": 200, "headers": RESPONSE_HEADERS, "body": json.dumps(response)}
    ui_print(res)
    return res
    
def get_score_file(event):
    try:
        body = json.loads(event["body"])
        ui_print(body)
        score_limit = int(body["score"])
        project_limit = body["project"]
        # 查询文件
        response = REPO_CODE_REVIEW_SCORE_TABLE.query(
            IndexName='version_file_index',
            KeyConditionExpression=Key('version').eq(0) & Key('project_branch_file').begins_with(project_limit),
            FilterExpression=Attr('score').lt(score_limit) & Attr('commit_id').ne("00000000")
        )
        print(response)
        
        # 获取需要文件和分数
        items = response['Items']
        
        return return_score_files(
            status="success",
            data=json.dumps(items, default=str)
        )
                    
    except Exception as e:
        ui_print(str(e))
        return return_score_files(status="failure", message=str(e))
        

def get_review_files(event):
    try:
        body = json.loads(event["body"])
        ui_print(body)
        reviewid = body["reviewid"]
        # 查询文件
        response = REPO_CODE_REVIEW_SCORE_TABLE.query(
            IndexName='review_id_gsi',
            KeyConditionExpression=Key('review_id').eq(reviewid),
            FilterExpression=Attr('version').ne(0)
        )
        print(response)
        
        # 获取需要文件和分数
        items = response['Items']
        
        return return_score_files(
            status="success",
            data=json.dumps(items, default=str)
        )
                    
    except Exception as e:
        ui_print(str(e))
        return return_score_files(status="failure", message=str(e))


def query_with_count(
    index_name,
    partition_key,
    partition_value,
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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
    
    query_params["FilterExpression"] = "NOT version = :version"
    expression_attribute_values[":version"] = {"N": "0"}
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
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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

    query_params["FilterExpression"] = "NOT version = :version"
    expression_attribute_values[":version"] = {"N": "0"}
    query_params["ExpressionAttributeValues"].update(expression_attribute_values)
        
    index_count = 0
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
        

def count_next_month(
    index_name,
    partition_key,
    partition_value,
    query_params,
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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
    
    query_params["FilterExpression"] = "NOT version = :version"
    expression_attribute_values[":version"] = {"N": "0"}
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
    print(res)
    ui_print(res)
    return res


def full_scan_next_month(
    index_name,
    partition_key,
    partition_value,
    query_params,
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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
    table_name=REPO_CODE_REVIEW_SCORE_TABLE_NAME,
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

    query_params["FilterExpression"] = "NOT version = :version"
    expression_attribute_values[":version"] = {"N": "0"}
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


def get_file_record(event):
    try:
        body = json.loads(event["body"])
        ui_print(body)  
        print(body)
        page_index = body.get("page_index", 0)
        print("page_index is " + str(page_index))
        page_size = body.get("page_size", 0)
        print("page_size is "+ str(page_size))
        
        
        if page_index <= 0 or page_size <= 0:
            return return_review_records(
                status="failure",
                message="page_index and page_size should be larger than 0.",
            )
            
        project = body.get("project", "")
        print("project is " + project)
        branch = body.get("branch", "")
        print("branch is " + branch)
        file_name = body.get("file_list", "")
        print("file name is " + file_name)

        # Query based on the file_name
        if project != "" and branch != "" and file_name != "":
            project_branch_file = str(project + "_" + branch + "_" + file_name)
            print("project_branch_file name is " + project_branch_file)
            index_name = "file_review_at_gsi"
            count = query_with_count(
                index_name,
                "project_branch_file",
                project_branch_file,
            )
            ui_print(f"total_records :{count}")
            print("total_records is " + str(count))
            if count > 0:
                last_page_index = max(page_index - 1, 0)
                if count <= last_page_index * page_size:
                    return_review_records(
                        status="failure",
                        message="page_index and page_size should be valid.",
                    )
                print("start query items.")
                response = query_with_pagination(
                    index_name,
                    "project_branch_file",
                    project_branch_file,
                    page_index,
                    page_size,
                )
                print(response["Items"])

                if response and response.get("Items"):
                    print("get items!")
                    return return_review_records(
                        status="success",
                        total_records=count,
                        data=response["Items"],
                    )
            elif count == 0:
                return return_review_records(
                        status="success",
                        total_records=count,
                        data=[],
                )

        count = count_items_in_dynamodb('year_month-review_at-index', 'year_month', datetime.now().strftime('%Y-%m'))
        print("full scan count is " + str(count))

        if count > 0:
            last_page_index = max(page_index - 1, 0)
            if count <= last_page_index * page_size:
                return_review_records(
                    status="failure",
                    message="page_index and page_size should be valid.",
                )
            page_items = full_table_scan(
                'year_month-review_at-index',
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
    
    if path == "/getScoreFile":
        return get_score_file(event)
    elif path == "/getReviewFiles":
        return get_review_files(event)
    elif path == "/getFileRecords":
        return get_file_record(event)
    else:
        res = {
            "statusCode": 404,
            "headers": RESPONSE_HEADERS,
            "body": json.dumps("API endpoint not found"),
        }
        ui_print(res)
        return res
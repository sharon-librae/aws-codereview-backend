import boto3
import os
from datetime import datetime

def add_column(table, id, version, review_at):
    # 扫描表获取所有记录
    response = table.scan()
    items = response['Items']
    
    # 继续扫描直到所有记录被获取
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    # 遍历记录并添加年-月列
    for item in items:
        # 假设记录中有一个日期时间列，例如 'created_at'
        created_at = item.get(review_at)
        if created_at:
            created_at = datetime.strptime(item.get(review_at).split(' ')[0], "%Y-%m-%d")
            year_month = created_at.strftime('%Y-%m')
            if version == '':
                table.update_item(
                    Key={id: item[id]},
                    UpdateExpression='SET year_month = :year_month',
                    ExpressionAttributeValues={':year_month': year_month}
                )
            else:
                table.update_item(
                    Key={id: item[id], version: item[version]},
                    UpdateExpression='SET year_month = :year_month',
                    ExpressionAttributeValues={':year_month': year_month}
                )
            print(f"Added year-month column for item with id {item[id]}")

def lambda_handler(event, context):
    # 初始化DynamoDB资源
    dynamodb = boto3.resource('dynamodb')
    
    # 指定现有DynamoDB表名
    REPO_CODE_REVIEW_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_TABLE_NAME")
    REPO_CODE_REVIEW_TABLE = dynamodb.Table(REPO_CODE_REVIEW_TABLE_NAME)
    
    # 扫描表获取所有记录
    add_column(REPO_CODE_REVIEW_TABLE, 'review_id', '', 'created_at')
    
    # 指定现有DynamoDB表名
    REPO_CODE_REVIEW_SCORE_TABLE_NAME = os.getenv("REPO_CODE_REVIEW_SCORE_TABLE_NAME")
    REPO_CODE_REVIEW_SCORE_TABLE = dynamodb.Table(REPO_CODE_REVIEW_SCORE_TABLE_NAME)
    
    # 扫描表获取所有记录
    add_column(REPO_CODE_REVIEW_SCORE_TABLE, 'project_branch_file', 'version', 'review_at')
    
    print('Year-month column added successfully for all records.')
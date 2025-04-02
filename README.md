## DEPLOYMENT GUIDE

### 部署包含三个部分
1. CDK部署前准备
2. CDK部署
3. CDK部署后操作

---

## 1. CDK部署前准备

### 1.1 权限与资源配置
- 通过角色授予部署EC2管理员权限
- EC2区域要求：us-east-1
- 推荐EC2配置：
  - 实例类型：t2.large
  - 系统：Ubuntu 22.04 LTS
  - 存储：EBS 200GB
- 申请Bedrock服务权限（us-east-1区域）

### 1.2 环境准备
```bash
sudo apt update
sudo apt install awscli
aws configure  # 选择us-east-1区域
sudo apt install npm
sudo npm install -g n
sudo n latest
sudo npm -v
sudo npm install -g aws-cdk
python3 --version  # 需Python 3.10.12
wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc
nvm install 18.18.0
nvm use 18.18.0
node -v
```

### 1.3 代码包部署
```bash
tar xvf aws-codereview.tar.gz
cd aws-codereview
python3 -m venv .venv
source .venv/bin/activate
bash prepare.sh
```

## 2. CDK部署

#### 每个区域只需执行一次
```bash
cdk bootstrap aws://<your-account>/us-east-1
cdk deploy
```

## 3. CDK部署后操作
### 3.1 Lambda配置

通过控制台将以下Lambda函数加入预配置的VPC（需包含NAT网关并加入白名单）：
- get_result_dev
- code_review_dev
- split_task_dev
- code_review_post_dev

### 3.2 VPC端点配置

在预配置VPC中创建以下端点：
- bedrock-runtime	接口端点
- sqs	接口端点
- dynamodb	网关端点
- s3	网关端点

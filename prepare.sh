  pip3 install -r requirements.txt
  mkdir -p codereview/lambda_layer
  pip3 install boto3==1.34.79 -t codereview/lambda_layer/boto3-new/python
  pip3 install python-gitlab==4.4.0 -t codereview/lambda_layer/gitlab/python
  pip3 install jinja2==3.1.3 -t codereview/lambda_layer/jinja2/python
  pip3 install pygments==2.17.2 -t codereview/lambda_layer/pygments/python
# Assumes a localstack is runing, make sure to start it by doing `localstack start`
awslocal s3 mb s3://test-bucket
mkdir -p build
pip install -r requirements.txt -t build/
cp lambda_function.py extractor.py -t build/
cp -r utils mq/ build/
cd build && zip -r ../function.zip . && cd ..



echo "Creating lambda function..."

awslocal lambda create-function \
  --function-name s3-trigger-fn \
  --runtime python3.12 \
  --handler lambda_function.handler \
  --zip-file fileb://function.zip \
  --role arn:aws:iam::000000000000:role/lambda-role

echo "Adding s3 -> lambda trigger..."

awslocal s3api put-bucket-notification-configuration \
  --bucket test-bucket \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "LambdaFunctionArn": "arn:aws:lambda:il-central-1:000000000000:function:s3-trigger-fn",
        "Events": ["s3:ObjectCreated:*"]
      }
    ]
  }'

echo "adding trigger premissions..."
awslocal lambda add-permission \
  --function-name s3-trigger-fn \
  --statement-id s3invoke \
  --action "lambda:InvokeFunction" \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::test-bucket

awslocal lambda update-function-configuration \
  --function-name s3-trigger-fn \
  --environment "Variables={RABBIT_HOST=host.docker.internal,RABBIT_PORT=5672,RABBIT_USER=myuser,RABBIT_PASS=mypass}"\
  --timeout 60 \
  --memory-size 512

rm -rf build/
rm function.zip

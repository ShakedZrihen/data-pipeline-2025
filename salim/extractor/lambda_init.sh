export AWS_PAGER=""
export AWS_DEFAULT_REGION=il-central-1
# Assumes a localstack is runing, make sure to start it by doing `localstack start`
awslocal s3 mb s3://test-bucket
mkdir -p build

pip install -r ./salim/extractor/lambda_requirements.txt -t build/
cp ./salim/extractor/lambda_function.py ./salim/extractor/extractor.py -t build/
cp -r ./salim/extractor/utils/ ./salim/extractor/mq/ ./salim/extractor/normalizer/ build/
cd build && zip -r ../function.zip . && cd ..



echo "Creating lambda function..."

awslocal lambda create-function \
  --function-name s3-trigger-fn \
  --runtime python3.12 \
  --handler lambda_function.handler \
  --zip-file fileb://function.zip \
  --role arn:aws:iam::000000000000:role/lambda-role


LAMBDA_ARN=$(awslocal lambda get-function --function-name s3-trigger-fn --query 'Configuration.FunctionArn' --output text)
echo "LAMBDA_ARN is $LAMBDA_ARN"
echo "adding lambda premissions..."
awslocal lambda add-permission \
  --function-name s3-trigger-fn \
  --statement-id s3invoke-$(date +%s) \
  --action "lambda:InvokeFunction" \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::test-bucket \
  --source-account 000000000000

echo "Adding s3 -> lambda trigger..."
awslocal s3api put-bucket-notification-configuration \
  --bucket test-bucket \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"LambdaFunctionArn\": \"${LAMBDA_ARN}\",
        \"Events\": [\"s3:ObjectCreated:*\"]

      }
    ]
  }"

awslocal lambda update-function-configuration \
  --function-name s3-trigger-fn \
  --environment "Variables={RABBIT_HOST=rabbitmq,RABBIT_PORT=5672,RABBIT_USER=myuser,RABBIT_PASS=mypass,RABBIT_VHOST=/}" \
  --timeout 60 \
  --memory-size 512


rm -rf build/
rm function.zip

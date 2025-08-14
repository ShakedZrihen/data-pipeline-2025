# bootstrap_localstack.ps1
# Script to create local S3 bucket and SQS queue in LocalStack

$endpoint = "http://localhost:4566"
$bucketName = "test-bucket"
$queueName = "my-queue"

Write-Host "Bootstrapping LocalStack resources..."
Write-Host "Creating S3 bucket: $bucketName"
awslocal --endpoint-url=$endpoint s3 mb s3://$bucketName

Write-Host "Creating SQS queue: $queueName"
awslocal --endpoint-url=$endpoint sqs create-queue --queue-name $queueName

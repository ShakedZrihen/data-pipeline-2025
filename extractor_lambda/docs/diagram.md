# Data Pipeline – Target Stage

```mermaid
flowchart LR
  S3[(S3: providers/<provider>/<branch>/<type>_<ts>.gz)]
  L1[Extractor Lambda<br/>handler.py]
  P[parser.py<br/>normalizer.py]
  Q[(SQS queue)]
  D[(DynamoDB:<br/>price-extractor-last-run)]
  C[Consumer Lambda<br/>sqs_consumer/handler.py<br/>print JSON]

  S3 --> L1 --> P --> Q
  L1 --> D
  Q --> C

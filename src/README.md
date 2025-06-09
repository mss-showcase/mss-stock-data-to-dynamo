# mss-stock-data-to-dynamo

Stock Data to DynamoDB lambda written in node.js

Reads json.gz files from S3 bucket (through CloudFront CDN) and load the records from JSON to DynamoDB.
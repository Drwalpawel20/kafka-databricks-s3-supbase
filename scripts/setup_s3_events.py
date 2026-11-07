"""Jednorazowa konfiguracja: kolejka SQS i powiadomienia S3 dla raw/orders*.parquet.
Uruchom lokalnie po uzupełnieniu .env: python scripts/setup_s3_events.py"""
import os
import json

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
BUCKET = os.environ["S3_BUCKET"]  # nazwa bucketu z .env
QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "s3-orders-events")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

sts = boto3.client(
    "sts",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# 1. Pobranie identyfikatora konta AWS
account_id = sts.get_caller_identity()["Account"]

# 2. Utworzenie kolejki SQS
try:
    queue_url = sqs.get_queue_url(
        QueueName=QUEUE_NAME
    )["QueueUrl"]

    print("Kolejka już istnieje.")

except ClientError as error:
    if error.response["Error"]["Code"] != "AWS.SimpleQueueService.NonExistentQueue":
        raise

    queue_url = sqs.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "VisibilityTimeout": "120",
            "ReceiveMessageWaitTimeSeconds": "20"
        }
    )["QueueUrl"]

    print("Utworzono kolejkę SQS.")

# 3. Pobranie ARN kolejki
queue_arn = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]

# 4. Polityka pozwalająca S3 wysyłać wiadomości do SQS
queue_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowS3ToSendMessages",
            "Effect": "Allow",
            "Principal": {
                "Service": "s3.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {
                "ArnLike": {
                    "aws:SourceArn": f"arn:aws:s3:::{BUCKET}"
                },
                "StringEquals": {
                    "aws:SourceAccount": account_id
                }
            }
        }
    ]
}

sqs.set_queue_attributes(
    QueueUrl=queue_url,
    Attributes={
        "Policy": json.dumps(queue_policy)
    }
)

# 5. Pobranie istniejącej konfiguracji powiadomień S3
notification_config = s3.get_bucket_notification_configuration(
    Bucket=BUCKET
)

notification_config.pop("ResponseMetadata", None)

# 6. Nie dodajemy drugi raz tej samej konfiguracji
queue_configs = notification_config.get("QueueConfigurations", [])

queue_configs = [
    config for config in queue_configs
    if config.get("Id") != "orders-parquet-created"
]

# 7. Konfiguracja zdarzenia dla nowych plików Parquet
queue_configs.append({
    "Id": "orders-parquet-created",
    "QueueArn": queue_arn,
    "Events": ["s3:ObjectCreated:*"],
    "Filter": {
        "Key": {
            "FilterRules": [
                {
                    "Name": "prefix",
                    "Value": "raw/orders"
                },
                {
                    "Name": "suffix",
                    "Value": ".parquet"
                }
            ]
        }
    }
})

notification_config["QueueConfigurations"] = queue_configs

# 8. Zapis konfiguracji
s3.put_bucket_notification_configuration(
    Bucket=BUCKET,
    NotificationConfiguration=notification_config
)

print("\nKonfiguracja zakończona!")
print("Bucket:", BUCKET)
print("Queue:", QUEUE_NAME)
print("Queue URL:", queue_url)
print("Queue ARN:", queue_arn)
print("Prefix: raw/orders")
print("Suffix: .parquet")

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import boto3
import requests
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG using the decorator
@dag(
    dag_id='sqs_message_collector_dag',
    default_args=default_args,
    description='Trigger API, monitor SQS messages, and submit solution',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sqs', 'api', 'boto3']
)
def sqs_monitor_dag():
    logger = LoggingMixin().log

    # Task to trigger the API and retrieve SQS URL
    @task()
    def trigger_api() -> str:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/ega9cw"
        try:
            logger.info("Triggering API to get SQS URL...")
            response = requests.post(url)
            response.raise_for_status()
            sqs_url = response.json().get("sqs_url")
            if not sqs_url:
                raise ValueError(f"No 'sqs_url' in API response: {response.json()}")
            logger.info(f"SQS Queue URL: {sqs_url}")
            return sqs_url
        except requests.RequestException as e:
            logger.error(f"Error triggering API: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    # Task to receive initial messages from the SQS queue
    @task()
    def receive_initial_messages(sqs_url: str):
        try:
            sqs = boto3.client("sqs", region_name="us-east-1")
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                VisibilityTimeout=30
            )
            messages = response.get("Messages", [])
            logger.info(f"Received {len(messages)} initial message(s).")
            for msg in messages:
                logger.info(f"Message ID: {msg['MessageId']} | Body: {msg['Body']}")
        except Exception as e:
            logger.error(f"Error receiving initial messages: {e}")
            raise

    # Task to monitor the SQS queue until all expected messages are received
    @task()
    def monitor_sqs_queue(sqs_url: str, expected_count: int = 21) -> dict:
        collected_data = {}
        received_ids = set()
        try:
            sqs = boto3.client("sqs", region_name="us-east-1")
            logger.info("Monitoring queue indefinitely until all messages are collected...")

            while len(collected_data) < expected_count:
                # Get queue attributes
                attrs = sqs.get_queue_attributes(
                    QueueUrl=sqs_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )["Attributes"]

                available = int(attrs.get("ApproximateNumberOfMessages", 0))
                logger.info(f"Collected {len(collected_data)}/{expected_count} | Available: {available}")

                if available > 0:
                    response = sqs.receive_message(
                        QueueUrl=sqs_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=5,
                        MessageAttributeNames=["All"],
                        VisibilityTimeout=60
                    )
                    messages = response.get("Messages", [])
                    if not messages:
                        logger.info("No messages returned despite availability. Waiting 5 seconds...")
                        time.sleep(5)
                        continue

                    # Process and delete each message
                    for msg in messages:
                        msg_id = msg["MessageId"]
                        if msg_id in received_ids:
                            continue
                        attributes = msg.get("MessageAttributes", {})
                        if "order_no" in attributes and "word" in attributes:
                            order_no = attributes["order_no"]["StringValue"]
                            word = attributes["word"]["StringValue"]
                            collected_data[order_no] = word
                            received_ids.add(msg_id)
                            logger.info(f"Received order_no={order_no}, word={word}")

                            # Delete message after processing
                            sqs.delete_message(
                                QueueUrl=sqs_url,
                                ReceiptHandle=msg["ReceiptHandle"]
                            )
                            logger.info(f"Deleted message {msg_id} from queue")
                        else:
                            logger.warning(f"Message missing expected attributes: {msg}")
                else:
                    logger.info("No messages available, waiting 10 seconds...")
                    time.sleep(10)

            logger.info("All messages received successfully.")
            return collected_data
        except Exception as e:
            logger.error(f"Error monitoring SQS queue: {e}")
            raise

    # Task to send the collected solution to the submission SQS queue
    @task()
    def send_solution(uvaid: str, collected_data: dict, platform: str):
        try:
            sqs = boto3.client("sqs", region_name="us-east-1")
            phrase = " ".join(collected_data[str(i)] for i in sorted(map(int, collected_data.keys())))
            response = sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
                MessageBody="Submitting solution",
                MessageAttributes={
                    'uvaid': {'DataType': 'String', 'StringValue': uvaid},
                    'phrase': {'DataType': 'String', 'StringValue': phrase},
                    'platform': {'DataType': 'String', 'StringValue': platform}
                }
            )
            logger.info(f"Submission response: {response}")
        except Exception as e:
            logger.error(f"Error sending solution: {e}")
            raise

    # Task to log and print the final ordered results
    @task()
    def summarize_results(collected_data: dict):
        try:
            logger.info("Final results:")
            for order_no in sorted(collected_data, key=lambda x: int(x)):
                logger.info(f"{order_no}: {collected_data[order_no]}")
            final_phrase = " ".join(collected_data[str(i)] for i in sorted(map(int, collected_data.keys())))
            print(f"Final ordered phrase: {final_phrase}")
        except Exception as e:
            logger.error(f"Error summarizing results: {e}")
            raise

    # -----------------------------
    # DAG Task Dependencies
    # -----------------------------
    sqs_url = trigger_api()
    initial_messages = receive_initial_messages(sqs_url)
    collected_data = monitor_sqs_queue(sqs_url)
    send_task = send_solution("ega9cw", collected_data, "airflow")
    summarize = summarize_results(collected_data)

    # Set proper execution order
    sqs_url >> initial_messages >> collected_data >> send_task >> summarize

# Instantiate the DAG
dag = sqs_monitor_dag()

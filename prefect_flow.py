from prefect import flow, task
import boto3
import requests
import time


@task
def populate_queue():
    """Populate SQS queue using API"""
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/ega9cw"
    payload = requests.post(url).json()
    print(f"API Response: {payload}")

    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        raise ValueError(f"API did not return 'sqs_url': {payload}")

    print(f"SQS Queue URL: {sqs_url}")
    return sqs_url


@task
def collect_messages(sqs_url, expected_count: int = 21):
    """Receive, parse, and delete messages after parsing (no timeout)"""
    sqs = boto3.client("sqs", region_name="us-east-1")

    collected_data = {}
    received_ids = set()

    while len(collected_data) < expected_count:
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]

        available = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))

        print(f"\nCollected {len(collected_data)}/{expected_count} messages")
        print(f"Available: {available}, In-flight: {in_flight}, Delayed: {delayed}")

        if available > 0:
            try:
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    VisibilityTimeout=60,
                    MessageAttributeNames=["All"],
                )

                messages = response.get("Messages", [])
                if not messages:
                    print("No messages returned despite availability. Waiting 5 seconds...")
                    time.sleep(5)
                    continue

                to_delete = []

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

                        to_delete.append({
                            "Id": msg_id,
                            "ReceiptHandle": msg["ReceiptHandle"],
                        })

                        print(f"Received order_no={order_no}, word={word}")
                    else:
                        print(f"Message missing expected attributes: {msg}")

                # batch delete the messages after parsing
                if to_delete:
                    try:
                        delete_response = sqs.delete_message_batch(
                            QueueUrl=sqs_url,
                            Entries=to_delete,
                        )
                        successful = len(delete_response.get("Successful", []))
                        failed = delete_response.get("Failed", [])
                        print(f"Batch deleted {successful} messages.")
                        if failed:
                            print(f"Failed deletions: {failed}")
                    except Exception as e:
                        print(f"Batch delete failed: {e}")

            except Exception as e:
                print(f"Error receiving messages: {e}")
                print("Waiting 10 seconds before retrying...")
                time.sleep(10)
        else:
            print("No messages available, waiting 10 seconds...")
            time.sleep(10)

    print("\nAll messages received, parsed, and deleted")
    return collected_data


@task
def reassemble_phrase(collected_data):
    """Reassemble words into phrase"""
    try:
        ordered_phrase = " ".join(
            collected_data[k] for k in sorted(collected_data, key=lambda x: int(x))
        )
    except Exception as e:
        raise ValueError(f"Error assembling phrase: {e}")

    print(f"\nReassembled phrase:\n{ordered_phrase}")
    return ordered_phrase


@task
def submit_solution(uvaid, phrase, platform="prefect"):
    """Send the assembled phrase to the submission queue"""
    sqs = boto3.client("sqs", region_name="us-east-1")
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        response = sqs.send_message(
            QueueUrl=submission_url,
            MessageBody="Pipeline submission for DP2",
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": uvaid},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": platform},
            },
        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            print("\nSubmission successful (HTTP 200)!")
            print(f"Submitted message ID: {response.get('MessageId')}")
            return True
        else:
            print(f"\nSubmission returned status code {status_code}")
            return False
    except Exception as e:
        print(f"Error submitting solution: {e}")
        return False


@flow(name="DP2 Prefect Pipeline")
def main_flow():
    sqs_url = populate_queue()
    collected_data = collect_messages(sqs_url)
    phrase = reassemble_phrase(collected_data)
    success = submit_solution("ega9cw", phrase)

    if not success:
        print("Submission failed. Check logs.")


if __name__ == "__main__":
    main_flow()

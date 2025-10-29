from prefect import flow, task
import boto3
import requests
import time


@task
def populate_queue():
    """Populate SQS queue using API"""
    # Define the API endpoint
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/ega9cw"
    
    # Make a POST request to the API and parse JSON response
    payload = requests.post(url).json()
    print(f"API Response: {payload}")

    # Extract the SQS queue URL from the API response
    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        # Raise an error if the API did not return the expected 'sqs_url'
        raise ValueError(f"API did not return 'sqs_url': {payload}")

    print(f"SQS Queue URL: {sqs_url}")
    # Return the queue URL for downstream tasks
    return sqs_url


@task
def collect_messages(sqs_url, expected_count: int = 21):
    """Receive, parse, and delete messages after parsing (no timeout)"""
    # Create an SQS client for interacting with the queue
    sqs = boto3.client("sqs", region_name="us-east-1")

    collected_data = {}  # Stores order_no -> word mapping
    received_ids = set()  # Track processed message IDs to avoid duplicates

    # Loop until we have collected the expected number of messages
    while len(collected_data) < expected_count:
        # Get queue attributes to monitor available, in-flight, and delayed messages
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
                # Receive messages from the queue (up to 10 at a time)
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    VisibilityTimeout=60,
                    MessageAttributeNames=["All"],
                )

                messages = response.get("Messages", [])
                if not messages:
                    # Handle rare case when no messages are returned
                    print("No messages returned despite availability. Waiting 5 seconds...")
                    time.sleep(5)
                    continue

                to_delete = []  # Keep track of messages to delete after processing

                # Process each received message
                for msg in messages:
                    msg_id = msg["MessageId"]
                    if msg_id in received_ids:
                        continue  # Skip messages already processed

                    attributes = msg.get("MessageAttributes", {})
                    # Only process messages containing the expected attributes
                    if "order_no" in attributes and "word" in attributes:
                        order_no = attributes["order_no"]["StringValue"]
                        word = attributes["word"]["StringValue"]

                        collected_data[order_no] = word
                        received_ids.add(msg_id)

                        # Prepare message for batch deletion
                        to_delete.append({
                            "Id": msg_id,
                            "ReceiptHandle": msg["ReceiptHandle"],
                        })

                        print(f"Received order_no={order_no}, word={word}")
                    else:
                        print(f"Message missing expected attributes: {msg}")

                # Batch delete the messages after parsing
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
                # Handle errors in receiving messages
                print(f"Error receiving messages: {e}")
                print("Waiting 10 seconds before retrying...")
                time.sleep(10)
        else:
            # No messages currently available, wait before retrying
            print("No messages available, waiting 10 seconds...")
            time.sleep(10)

    print("\nAll messages received, parsed, and deleted")
    return collected_data


@task
def reassemble_phrase(collected_data):
    """Reassemble words into phrase"""
    try:
        # Sort messages by order_no and join words into a single phrase
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
    # Initialize SQS client for submission queue
    sqs = boto3.client("sqs", region_name="us-east-1")
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        # Send message with attributes to the submission queue
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
    # Step 1: Populate the SQS queue via API
    sqs_url = populate_queue()
    
    # Step 2: Collect messages from the queue
    collected_data = collect_messages(sqs_url)
    
    # Step 3: Reassemble the phrase from collected messages
    phrase = reassemble_phrase(collected_data)
    
    # Step 4: Submit the reassembled phrase to the solution queue
    success = submit_solution("ega9cw", phrase)

    if not success:
        print("Submission failed. Check logs.")


if __name__ == "__main__":
    # Run the Prefect flow
    main_flow()

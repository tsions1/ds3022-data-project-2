# prefect flow goes here
from prefect import flow, task
import boto3
import requests
import time
from datetime import datetime


uvaid = "cnb8jw"  
platform = "prefect"
api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


@task
def populate_queue():
    # Calls API once to populate the SQS queue.
    try:
        payload = requests.post(api_url)
        payload.raise_for_status()
        data = payload.json()
        sqs_url = data["sqs_url"]
        log(f"Queue populated: {sqs_url}")
        return sqs_url
    
    except Exception as e:
        log(f"Error populating queue: {e}")
        return None


@task
def monitor_queue(sqs_url):
    # Monitors the queue until all 21 messages are ready.
    sqs = boto3.client("sqs")
    total = 0
    log("Monitoring queue for messages...")
    while total < 21:
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        visible = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
        not_visible = int(attrs["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
        delayed = int(attrs["Attributes"]["ApproximateNumberOfMessagesDelayed"])
        total = visible + not_visible + delayed

        log(f"Messages total: {total}/21")
        if total < 21:
            time.sleep(1)  # wait 1 second before checking again

    log("All messages are now available.")
    return True


@task
def collect_messages(sqs_url):
    # Retrieves and deletes all messages, only outputting the final quote
    sqs = boto3.client("sqs")
    words = []
    count = 0
    start_time = time.time()
    max_wait = 900  # maximum wait time of 15 minutes

    while count < 21:
        if time.time() - start_time > max_wait:
            log("Maximum wait exceeded, stopping collection for testing.")
            break

        response = sqs.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=5,
            WaitTimeSeconds=2,
        )
        if "Messages" not in response:
            log("Messages incoming...")
            time.sleep(4)
            continue

        for msg in response["Messages"]:
            attrb = msg.get("MessageAttributes", {})
            if "order_no" not in attrb or "word" not in attrb:
                continue

            order_no = int(attrb["order_no"]["StringValue"])
            word = attrb["word"]["StringValue"]
            words.append((order_no, word))

            # Delete message after processing
            sqs.delete_message(
                QueueUrl=sqs_url, 
                ReceiptHandle=msg["ReceiptHandle"]
            )
            count += 1
        
# Reassemble the quote
    words.sort(key=lambda x: x[0])
    phrase = " ".join([w for _, w in words])
    log(f"Reassembled quote ({count} words): {phrase}")
    return phrase

# send final assembled quote
def send_solution(uvaid, phrase, platform):
    sqs = boto3.client("sqs")
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    message = f"DP2 submission from {uvaid}"

    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        log(f"Submission response: {response}")
        return True
    except Exception as e:
        log(f"Error sending solution: {e}")
        return False

@flow(name="DP2 Prefect Flow")
def main_flow():
    sqs_url = populate_queue()
    if sqs_url:
        monitor_queue(sqs_url)
        phrase = collect_messages(sqs_url)
        log(f"Final Phrase: {phrase}")
        send_solution(uvaid, phrase, platform)

if __name__ == "__main__":
    main_flow()
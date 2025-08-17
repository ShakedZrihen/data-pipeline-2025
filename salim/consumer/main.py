import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from salim.consumer.sqs_consumer import receive_messages, delete_message
from salim.consumer.handle_message import handle_message

def process_message(msg):
    success = handle_message(msg)
    # if success:
    #     delete_message(msg["ReceiptHandle"])

def main():
    while True:
        messages = receive_messages()
        for msg in messages:
            process_message(msg)

if __name__ == "__main__":
    main()

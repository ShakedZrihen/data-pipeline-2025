from extractor import Extractor
from sqs.sqs_producer import receive_messages_from_sqs, send_message_to_sqs
import json
from utils import *

if __name__ == "__main__":
    extractor = Extractor()
    extractor.extract_from_s3()
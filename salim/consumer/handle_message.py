import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from salim.consumer.normalizer import normalize
from salim.consumer.enricher import enrich
from salim.consumer.validator import validate
from salim.consumer.db import persist
from salim.consumer.logger import logger
from salim.consumer.dlq_handler import send_to_dlq


def handle_message(raw_msg):
    """
    Processes a single raw SQS message:
    1. Parses JSON
    2. Normalizes to unified schema
    3. Enriches missing/derived fields
    4. Validates data
    5. Saves to PostgreSQL
    Sends to DLQ on failure.
    """

    try:
        body = json.loads(raw_msg["Body"])
        logger.debug(f"Raw message: {body}")

        # Step 1: Normalize to unified schema
        normalized = normalize(body)
        logger.debug(f"Normalized: {normalized}")

        # Step 2: Enrich missing/derived fields
        enriched = enrich(normalized)
        logger.debug(f"Enriched: {enriched}")

        # Step 3: Validate against schema
        validate(enriched)

        # Step 4: Save to PostgreSQL (upsert / idempotent)
        persist(enriched)

        logger.info(f"Successfully processed message from source={enriched.get('source')}")
        return True

    except Exception as e:
        logger.exception("Failed to process message. Sending to DLQ.")
        send_to_dlq(raw_msg, str(e))
        return False

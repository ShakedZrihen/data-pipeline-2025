CREATE UNIQUE INDEX IF NOT EXISTS ux_enriched_msg_raw
ON enriched_items(message_id, product_raw);

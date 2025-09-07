INSERT OR IGNORE INTO supermarkets(provider, branch, name_hint)
SELECT DISTINCT provider, branch, provider
FROM messages;


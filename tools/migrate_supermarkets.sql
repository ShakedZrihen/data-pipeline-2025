CREATE TABLE IF NOT EXISTS supermarkets (
  supermarket_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name          TEXT,
  branch_name   TEXT,
  city          TEXT,
  address       TEXT,
  website       TEXT,
  created_at    TEXT DEFAULT (datetime('now')),
  provider      TEXT NOT NULL,
  branch        TEXT NOT NULL,
  UNIQUE(provider, branch)
);

INSERT INTO supermarkets (name, website, provider, branch)
SELECT
  CASE lower(m.provider)
      WHEN 'keshet'     THEN 'Keshet'
      WHEN 'politzer'   THEN 'Politzer'
      WHEN 'yohananof'  THEN 'Yohananof'
      ELSE m.provider
  END AS name,
  CASE lower(m.provider)
      WHEN 'keshet'     THEN 'https://www.keshet.co.il/'
      WHEN 'yohananof'  THEN 'https://www.yohananof.co.il/'
      ELSE NULL
  END AS website,
  m.provider,
  m.branch
FROM (SELECT DISTINCT provider, branch FROM messages) AS m
LEFT JOIN supermarkets s
  ON s.provider = m.provider AND s.branch = m.branch
WHERE s.supermarket_id IS NULL;

CREATE VIEW IF NOT EXISTS message_supermarket_vw AS
SELECT m.id AS message_id, s.supermarket_id
FROM messages m
JOIN supermarkets s
  ON s.provider = m.provider AND s.branch = m.branch;

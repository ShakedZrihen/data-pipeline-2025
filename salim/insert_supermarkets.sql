-- Insert supermarkets data
INSERT INTO supermarkets (name, branch_name, city, address, website) VALUES 
('Rami Levi', NULL, NULL, NULL, 'https://www.rami-levy.co.il/'),
('Yohananof', NULL, NULL, NULL, 'https://www.yohananof.co.il/'),
('Carrefour', NULL, NULL, NULL, 'https://www.carrefour.co.il/')
ON CONFLICT DO NOTHING;

-- Display inserted data
SELECT * FROM supermarkets ORDER BY supermarket_id;
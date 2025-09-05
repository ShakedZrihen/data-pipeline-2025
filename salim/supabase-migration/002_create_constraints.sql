-- Migration: 002_create_constraints.sql
-- Description: Add constraints to prevent duplicate data
-- Date: 2025-09-03

-- Add unique constraint to prevent duplicate products from same file
-- This ensures that the same product from the same provider/branch/file/timestamp cannot be inserted twice
ALTER TABLE supermarket_data 
ADD CONSTRAINT supermarket_data_provider_branch_file_type_file_timestamp_p_key 
UNIQUE (provider, branch, file_type, file_timestamp, product_name);

-- Add check constraints for data integrity
ALTER TABLE supermarket_data 
ADD CONSTRAINT check_price_positive 
CHECK (price >= 0);

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_unit_price_positive 
CHECK (unit_price IS NULL OR unit_price >= 0);

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_min_quantity_positive 
CHECK (min_quantity IS NULL OR min_quantity >= 0);

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_price_per_kg_positive 
CHECK (price_per_kg IS NULL OR price_per_kg >= 0);

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_price_per_liter_positive 
CHECK (price_per_liter IS NULL OR price_per_liter >= 0);

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_file_type_valid 
CHECK (file_type IN ('pricesFull', 'promoFull'));

ALTER TABLE supermarket_data 
ADD CONSTRAINT check_total_items_positive 
CHECK (total_items_in_file > 0);

-- Add constraint comments
COMMENT ON CONSTRAINT supermarket_data_provider_branch_file_type_file_timestamp_p_key ON supermarket_data 
IS 'Prevents duplicate products from the same file/timestamp';

COMMENT ON CONSTRAINT check_price_positive ON supermarket_data 
IS 'Ensures product prices are not negative';

COMMENT ON CONSTRAINT check_file_type_valid ON supermarket_data 
IS 'Ensures file_type is either pricesFull or promoFull';
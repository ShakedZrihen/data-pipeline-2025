-- Migration: 001_create_supermarket_data_table.sql
-- Description: Create the main supermarket_data table with all columns
-- Date: 2025-09-03

CREATE TABLE IF NOT EXISTS supermarket_data (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Core identification fields (NOT NULL)
    provider VARCHAR(100) NOT NULL,
    branch VARCHAR(200) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    file_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    product_name VARCHAR(500) NOT NULL,
    
    -- Product details (nullable)
    product_code VARCHAR(100),
    manufacturer VARCHAR(200),
    
    -- Pricing information
    price NUMERIC NOT NULL,
    unit VARCHAR(50) NOT NULL DEFAULT 'unit',
    unit_price NUMERIC,
    min_quantity NUMERIC,
    
    -- Additional pricing calculations
    price_per_kg NUMERIC,
    price_per_liter NUMERIC,
    
    -- Classification fields
    is_promotion BOOLEAN NOT NULL DEFAULT FALSE,
    is_kosher BOOLEAN,
    category VARCHAR(100),
    
    -- Processing metadata
    message_id VARCHAR(100) NOT NULL,
    total_items_in_file INTEGER NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Add comment to table
COMMENT ON TABLE supermarket_data IS 'Stores supermarket product data from price and promotion files';

-- Add column comments
COMMENT ON COLUMN supermarket_data.provider IS 'Supermarket chain name (e.g., politzer)';
COMMENT ON COLUMN supermarket_data.branch IS 'Specific branch location';
COMMENT ON COLUMN supermarket_data.file_type IS 'Type of file: pricesFull or promoFull';
COMMENT ON COLUMN supermarket_data.file_timestamp IS 'Timestamp from the original XML file';
COMMENT ON COLUMN supermarket_data.product_name IS 'Product name in Hebrew/original language';
COMMENT ON COLUMN supermarket_data.is_promotion IS 'True if this is from a promoFull file';
COMMENT ON COLUMN supermarket_data.message_id IS 'SQS message ID that processed this batch';
COMMENT ON COLUMN supermarket_data.total_items_in_file IS 'Total number of items in the source file';
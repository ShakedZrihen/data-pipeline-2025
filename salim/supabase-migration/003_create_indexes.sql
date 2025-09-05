-- Migration: 003_create_indexes.sql
-- Description: Create performance indexes for common queries
-- Date: 2025-09-03

-- Composite index for filtering by provider and branch
CREATE INDEX IF NOT EXISTS idx_supermarket_data_provider_branch 
ON supermarket_data (provider, branch);

-- Index for file type filtering (pricesFull vs promoFull)
CREATE INDEX IF NOT EXISTS idx_supermarket_data_file_type 
ON supermarket_data (file_type);

-- Index for time-based queries on processed_at
CREATE INDEX IF NOT EXISTS idx_supermarket_data_processed_at 
ON supermarket_data (processed_at);

-- Index for product name searches
CREATE INDEX IF NOT EXISTS idx_supermarket_data_product_name 
ON supermarket_data (product_name);

-- Index for price range queries
CREATE INDEX IF NOT EXISTS idx_supermarket_data_price 
ON supermarket_data (price);

-- Index for message tracking
CREATE INDEX IF NOT EXISTS idx_supermarket_data_message_id 
ON supermarket_data (message_id);

-- Composite index for time-series analysis by provider/branch
CREATE INDEX IF NOT EXISTS idx_supermarket_data_provider_branch_timestamp 
ON supermarket_data (provider, branch, file_timestamp);

-- Index for promotion filtering
CREATE INDEX IF NOT EXISTS idx_supermarket_data_is_promotion 
ON supermarket_data (is_promotion);

-- Index for category filtering (when populated)
CREATE INDEX IF NOT EXISTS idx_supermarket_data_category 
ON supermarket_data (category) 
WHERE category IS NOT NULL;

-- Partial index for kosher products (when populated)
CREATE INDEX IF NOT EXISTS idx_supermarket_data_kosher 
ON supermarket_data (is_kosher) 
WHERE is_kosher IS NOT NULL;

-- Composite index for efficient data cleanup queries
CREATE INDEX IF NOT EXISTS idx_supermarket_data_cleanup 
ON supermarket_data (created_at, id);

-- Index comments
COMMENT ON INDEX idx_supermarket_data_provider_branch IS 'Optimizes queries filtering by supermarket chain and branch';
COMMENT ON INDEX idx_supermarket_data_processed_at IS 'Optimizes time-based queries and data cleanup operations';
COMMENT ON INDEX idx_supermarket_data_product_name IS 'Optimizes product name searches and filtering';
COMMENT ON INDEX idx_supermarket_data_price IS 'Optimizes price range queries and analytics';
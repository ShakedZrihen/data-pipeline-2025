# Supabase Database Migrations

This directory contains PostgreSQL migration files for the supermarket data pipeline.

## Files

### Migration Files
1. **001_create_supermarket_data_table.sql** - Creates the main `supermarket_data` table with all columns
2. **002_create_constraints.sql** - Adds data integrity constraints and unique constraints
3. **003_create_indexes.sql** - Creates performance indexes for common queries

### Python Scripts
- **run_migrations.py** - Migration runner with validation
- **requirements.txt** - Python dependencies
- **.env.example** - Environment configuration template

## Quick Start (Python - Recommended)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your Supabase credentials
# SUPABASE_DB_HOST=db.your-project-ref.supabase.co
# SUPABASE_DB_PASSWORD=your-database-password
```

### 3. Run Setup
```bash
python run_migrations.py
```

This will:
- Run all migrations in order
- Validate schema is correct
- Test data insertion/retrieval
- Confirm database is "ready to go"

## Alternative Methods

### Option 1: Supabase SQL Editor
1. Open Supabase Dashboard → SQL Editor
2. Copy and paste each migration file content
3. Run them in order (001, 002, 003)

### Option 2: psql Command Line
```bash
psql -h your-supabase-host -U postgres -d postgres -f 001_create_supermarket_data_table.sql
psql -h your-supabase-host -U postgres -d postgres -f 002_create_constraints.sql
psql -h your-supabase-host -U postgres -d postgres -f 003_create_indexes.sql
```

## Table Schema

The `supermarket_data` table includes:

- **Core fields**: provider, branch, file_type, file_timestamp, product_name
- **Product details**: product_code, manufacturer, price, unit
- **Additional pricing**: unit_price, min_quantity, price_per_kg, price_per_liter  
- **Classification**: is_promotion, is_kosher, category
- **Metadata**: message_id, total_items_in_file, processed_at, created_at, updated_at

## Key Features

- **Unique constraint** prevents duplicate products from same file
- **Performance indexes** for common query patterns
- **Data validation** ensures prices are positive and file_types are valid
- **Timestamps** for audit trail and data cleanup
- **Comments** for documentation

## Rollback

To rollback migrations (in reverse order):

```sql
-- Drop indexes
DROP INDEX IF EXISTS idx_supermarket_data_provider_branch;
-- ... (drop other indexes)

-- Drop constraints  
ALTER TABLE supermarket_data DROP CONSTRAINT IF EXISTS check_price_positive;
-- ... (drop other constraints)

-- Drop table
DROP TABLE IF EXISTS supermarket_data;
```
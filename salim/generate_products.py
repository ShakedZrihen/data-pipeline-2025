#!/usr/bin/env python3
"""
Script to generate 1000 products for each supermarket with realistic prices.
Includes foundation products like milk, bread, etc. with price variations.
"""

import random
import hashlib

# Foundation products that should exist in all supermarkets
FOUNDATION_PRODUCTS = [
    # Dairy
    ("Milk 1L", "Tara", "Dairy", 1.0, "l", "7290000000001"),
    ("Milk 3% 1L", "Tnuva", "Dairy", 1.0, "l", "7290000000002"),
    ("Yogurt 150g", "Danone", "Dairy", 150, "g", "7290000000003"),
    ("Cottage Cheese 250g", "Tnuva", "Dairy", 250, "g", "7290000000004"),
    ("Butter 200g", "Tnuva", "Dairy", 200, "g", "7290000000005"),
    ("Cheese Slices 200g", "Eshel", "Dairy", 200, "g", "7290000000006"),
    
    # Bakery
    ("White Bread 750g", "Berman", "Bakery", 750, "g", "7290000000010"),
    ("Whole Wheat Bread 500g", "Angel", "Bakery", 500, "g", "7290000000011"),
    ("Pita Bread 6 units", "Angel", "Bakery", 6, "unit", "7290000000012"),
    ("Bagels 4 units", "Berman", "Bakery", 4, "unit", "7290000000013"),
    
    # Meat & Poultry
    ("Chicken Breast 1kg", "Of Tov", "Meat", 1.0, "kg", "7290000000020"),
    ("Ground Beef 500g", "Tirat Tzvi", "Meat", 500, "g", "7290000000021"),
    ("Turkey Slices 150g", "Tirat Tzvi", "Meat", 150, "g", "7290000000022"),
    
    # Fruits & Vegetables
    ("Bananas 1kg", "", "Produce", 1.0, "kg", "7290000000030"),
    ("Apples 1kg", "", "Produce", 1.0, "kg", "7290000000031"),
    ("Tomatoes 1kg", "", "Produce", 1.0, "kg", "7290000000032"),
    ("Cucumbers 1kg", "", "Produce", 1.0, "kg", "7290000000033"),
    ("Onions 1kg", "", "Produce", 1.0, "kg", "7290000000034"),
    ("Potatoes 2kg", "", "Produce", 2.0, "kg", "7290000000035"),
    ("Carrots 1kg", "", "Produce", 1.0, "kg", "7290000000036"),
    
    # Pantry Staples
    ("Rice 1kg", "Sugat", "Pantry", 1.0, "kg", "7290000000040"),
    ("Pasta 500g", "Barilla", "Pantry", 500, "g", "7290000000041"),
    ("Olive Oil 500ml", "Mishaan", "Pantry", 500, "ml", "7290000000042"),
    ("Sugar 1kg", "Dor", "Pantry", 1.0, "kg", "7290000000043"),
    ("Salt 1kg", "Carmel", "Pantry", 1.0, "kg", "7290000000044"),
    ("Black Pepper 50g", "Pereg", "Pantry", 50, "g", "7290000000045"),
    
    # Beverages
    ("Orange Juice 1L", "Tapuzina", "Beverages", 1.0, "l", "7290000000050"),
    ("Coca Cola 1.5L", "Coca Cola", "Beverages", 1.5, "l", "7290000000051"),
    ("Mineral Water 1.5L", "Mei Eden", "Beverages", 1.5, "l", "7290000000052"),
    ("Coffee 200g", "Elite", "Beverages", 200, "g", "7290000000053"),
    ("Tea Bags 25 units", "Wissotzky", "Beverages", 25, "unit", "7290000000054"),
    
    # Eggs & Basic Proteins
    ("Eggs Large 12 units", "Tnuva", "Dairy", 12, "unit", "7290000000060"),
    ("Canned Tuna 160g", "Starkist", "Pantry", 160, "g", "7290000000061"),
]

# Additional product categories to reach 1000 products
ADDITIONAL_PRODUCTS = [
    # Snacks
    ("Bamba 50g", "Osem", "Snacks", 50, "g"),
    ("Chips 150g", "Tapuchips", "Snacks", 150, "g"),
    ("Chocolate Bar 100g", "Elite", "Snacks", 100, "g"),
    ("Cookies 250g", "Osem", "Snacks", 250, "g"),
    ("Pretzels 200g", "Osem", "Snacks", 200, "g"),
    
    # Frozen Foods
    ("Frozen Pizza 400g", "Dr. Oetker", "Frozen", 400, "g"),
    ("Ice Cream 1L", "Strauss", "Frozen", 1.0, "l"),
    ("Frozen Vegetables 800g", "Tiv Taam", "Frozen", 800, "g"),
    
    # Cleaning & Household
    ("Dish Soap 500ml", "Fairy", "Household", 500, "ml"),
    ("Laundry Detergent 3L", "Ariel", "Household", 3.0, "l"),
    ("Toilet Paper 12 rolls", "Sano", "Household", 12, "unit"),
    ("Kitchen Towels 2 rolls", "Sano", "Household", 2, "unit"),
    
    # Personal Care
    ("Shampoo 400ml", "Head & Shoulders", "Personal Care", 400, "ml"),
    ("Toothpaste 75ml", "Colgate", "Personal Care", 75, "ml"),
    ("Soap Bar 125g", "Dove", "Personal Care", 125, "g"),
]

# Base prices for foundation products (in ILS)
BASE_PRICES = {
    "7290000000001": 5.90,  # Milk 1L
    "7290000000002": 6.20,  # Milk 3% 1L
    "7290000000003": 3.90,  # Yogurt
    "7290000000004": 7.50,  # Cottage Cheese
    "7290000000005": 12.90, # Butter
    "7290000000006": 15.90, # Cheese Slices
    "7290000000010": 4.50,  # White Bread
    "7290000000011": 5.90,  # Whole Wheat Bread
    "7290000000012": 3.50,  # Pita Bread
    "7290000000013": 6.90,  # Bagels
    "7290000000020": 39.90, # Chicken Breast
    "7290000000021": 35.90, # Ground Beef
    "7290000000022": 18.90, # Turkey Slices
    "7290000000030": 7.90,  # Bananas
    "7290000000031": 8.90,  # Apples
    "7290000000032": 6.90,  # Tomatoes
    "7290000000033": 4.90,  # Cucumbers
    "7290000000034": 3.90,  # Onions
    "7290000000035": 6.90,  # Potatoes
    "7290000000036": 4.90,  # Carrots
    "7290000000040": 12.90, # Rice
    "7290000000041": 8.90,  # Pasta
    "7290000000042": 18.90, # Olive Oil
    "7290000000043": 4.90,  # Sugar
    "7290000000044": 2.90,  # Salt
    "7290000000045": 8.90,  # Black Pepper
    "7290000000050": 8.90,  # Orange Juice
    "7290000000051": 6.90,  # Coca Cola
    "7290000000052": 3.90,  # Mineral Water
    "7290000000053": 24.90, # Coffee
    "7290000000054": 12.90, # Tea Bags
    "7290000000060": 14.90, # Eggs
    "7290000000061": 6.90,  # Canned Tuna
}

def generate_barcode(name, brand, category):
    """Generate a consistent barcode based on product details"""
    base_string = f"{name}_{brand}_{category}"
    hash_object = hashlib.md5(base_string.encode())
    # Take first 13 digits from hash and ensure it starts with 729 (Israel prefix)
    hash_hex = hash_object.hexdigest()
    barcode = "729" + ''.join([str(int(c, 16) % 10) for c in hash_hex[:10]])
    return barcode

def get_price_variation(base_price, supermarket_name):
    """Apply realistic price variations between supermarkets"""
    variations = {
        "Rami Levi": 0.95,    # Generally 5% cheaper
        "Yohananof": 1.02,    # Generally 2% more expensive
        "Carrefour": 1.0      # Base prices
    }
    
    # Add some randomness to make it more realistic
    random_factor = random.uniform(0.95, 1.05)
    
    return round(base_price * variations.get(supermarket_name, 1.0) * random_factor, 2)

def generate_products_sql():
    """Generate SQL insert statements for all products"""
    
    supermarkets = {
        1: "Rami Levi",
        2: "Yohananof", 
        3: "Carrefour"
    }
    
    sql_statements = []
    
    for supermarket_id, supermarket_name in supermarkets.items():
        print(f"Generating products for {supermarket_name}...")
        
        # Add foundation products (these exist in all supermarkets)
        for name, brand, category, size_value, size_unit, barcode in FOUNDATION_PRODUCTS:
            base_price = BASE_PRICES.get(barcode, random.uniform(5, 50))
            price = get_price_variation(base_price, supermarket_name)
            
            # Some products might have promotions
            promo_price = None
            promo_text = None
            if random.random() < 0.1:  # 10% chance of promotion
                promo_price = round(price * 0.85, 2)  # 15% off
                promo_text = "Special Offer!"
            
            sql = f"""INSERT INTO products (supermarket_id, barcode, canonical_name, brand, category, size_value, size_unit, price, currency, promo_price, promo_text, loyalty_only, in_stock, source, raw_hash) VALUES ({supermarket_id}, '{barcode}', '{name}', '{brand}', '{category}', {size_value}, '{size_unit}', {price}, 'ILS', {promo_price or 'NULL'}, {'NULL' if not promo_text else f"'{promo_text}'"}, {random.choice(['true', 'false'])}, {random.choice(['true', 'true', 'true', 'false'])}, 'generated', '{hashlib.md5(f"{supermarket_id}{barcode}".encode()).hexdigest()}');"""
            
            sql_statements.append(sql)
        
        # Add additional products to reach ~1000 per supermarket
        products_added = len(FOUNDATION_PRODUCTS)
        
        while products_added < 1000:
            # Pick a random additional product template
            template = random.choice(ADDITIONAL_PRODUCTS)
            name_base, brand, category, size_value, size_unit = template
            
            # Vary the name slightly to create different products
            variations = [
                f"{name_base}",
                f"{name_base} Premium",
                f"{name_base} Light",
                f"{name_base} Organic",
                f"{name_base} Family Size",
            ]
            
            name = random.choice(variations)
            barcode = generate_barcode(name, brand, category)
            
            # Generate a reasonable price based on category
            category_price_ranges = {
                "Snacks": (3, 15),
                "Frozen": (8, 25),
                "Household": (5, 40),
                "Personal Care": (8, 35),
                "Beverages": (3, 20),
                "Pantry": (4, 30),
            }
            
            price_range = category_price_ranges.get(category, (5, 25))
            base_price = random.uniform(*price_range)
            price = get_price_variation(base_price, supermarket_name)
            
            # Some products might have promotions
            promo_price = None
            promo_text = None
            if random.random() < 0.12:  # 12% chance of promotion
                promo_price = round(price * random.uniform(0.7, 0.9), 2)
                promo_text = random.choice(["Sale!", "Limited Time!", "Special Price!", "Buy Now!"])
            
            sql = f"""INSERT INTO products (supermarket_id, barcode, canonical_name, brand, category, size_value, size_unit, price, currency, promo_price, promo_text, loyalty_only, in_stock, source, raw_hash) VALUES ({supermarket_id}, '{barcode}', '{name}', '{brand}', '{category}', {size_value}, '{size_unit}', {price}, 'ILS', {promo_price or 'NULL'}, {'NULL' if not promo_text else f"'{promo_text}'"}, {random.choice(['true', 'false'])}, {random.choice(['true', 'true', 'true', 'false'])}, 'generated', '{hashlib.md5(f"{supermarket_id}{barcode}".encode()).hexdigest()}');"""
            
            sql_statements.append(sql)
            products_added += 1
    
    return sql_statements

if __name__ == "__main__":
    print("Generating product data...")
    random.seed(42)  # For reproducible results
    
    sql_statements = generate_products_sql()
    
    # Write to file
    with open("insert_products.sql", "w", encoding="utf-8") as f:
        f.write("-- Generated product data for supermarkets\n")
        f.write("-- Foundation products exist in all supermarkets with price variations\n\n")
        
        for sql in sql_statements:
            f.write(sql + "\n")
        
        f.write(f"\n-- Total products generated: {len(sql_statements)}\n")
        f.write("-- Query to see product count per supermarket:\n")
        f.write("-- SELECT s.name, COUNT(p.product_id) as product_count FROM supermarkets s LEFT JOIN products p ON s.supermarket_id = p.supermarket_id GROUP BY s.supermarket_id, s.name;\n")
    
    print(f"Generated {len(sql_statements)} product records")
    print("SQL file saved as 'insert_products.sql'")
    print("Run: docker-compose exec db psql -U postgres -d salim_db -f /path/to/insert_products.sql")
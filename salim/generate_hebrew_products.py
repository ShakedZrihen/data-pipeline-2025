#!/usr/bin/env python3
"""
Hebrew Product Data Generator for Israeli Supermarkets
Generates realistic Hebrew product names with Israeli market prices
"""

import psycopg2
import random
from datetime import datetime, timedelta
import hashlib

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'salim_db',
    'user': 'postgres',
    'password': 'postgres'
}

# Hebrew product categories with realistic Israeli products
HEBREW_PRODUCTS = {
    'חלב ומוצריו': [
        # Milk products
        {'name': 'חלב טרי 3% שומן ליטר', 'brand': 'תנובה', 'size': (1.0, 'ליטר'), 'base_price': 6.90, 'barcode_prefix': '7290000'},
        {'name': 'חלב טרי 1% שומן ליטר', 'brand': 'תנובה', 'size': (1.0, 'ליטר'), 'base_price': 6.90, 'barcode_prefix': '7290001'},
        {'name': 'חלב טרי 3% שומן ליטר', 'brand': 'טרה', 'size': (1.0, 'ליטר'), 'base_price': 6.50, 'barcode_prefix': '7290002'},
        {'name': 'חלב טרי במרקם קרמי ליטר', 'brand': 'שטראוס', 'size': (1.0, 'ליטר'), 'base_price': 7.20, 'barcode_prefix': '7290003'},
        {'name': 'חלב ללא לקטוז ליטר', 'brand': 'תנובה', 'size': (1.0, 'ליטר'), 'base_price': 8.90, 'barcode_prefix': '7290004'},
        {'name': 'גבינה צהובה פלחים', 'brand': 'תנובה', 'size': (200, 'גרם'), 'base_price': 14.90, 'barcode_prefix': '7290005'},
        {'name': 'גבינה לבנה 5%', 'brand': 'תנובה', 'size': (250, 'גרם'), 'base_price': 7.50, 'barcode_prefix': '7290006'},
        {'name': 'יוגורט טבעי', 'brand': 'דנונה', 'size': (150, 'גרם'), 'base_price': 3.90, 'barcode_prefix': '7290007'},
        {'name': 'יוגורט ביו עם פירות', 'brand': 'דנונה', 'size': (125, 'גרם'), 'base_price': 4.50, 'barcode_prefix': '7290008'},
        {'name': 'חמאה מלוחה', 'brand': 'תנובה', 'size': (200, 'גרם'), 'base_price': 9.90, 'barcode_prefix': '7290009'},
    ],
    'לחם ומאפים': [
        {'name': 'לחם לבן פרוס', 'brand': 'ברמן', 'size': (750, 'גרם'), 'base_price': 4.50, 'barcode_prefix': '7290010'},
        {'name': 'לחם מלא פרוס', 'brand': 'אנג\'ל', 'size': (500, 'גרם'), 'base_price': 6.90, 'barcode_prefix': '7290011'},
        {'name': 'לחם שיפון מלא', 'brand': 'אנג\'ל', 'size': (400, 'גרם'), 'base_price': 8.50, 'barcode_prefix': '7290012'},
        {'name': 'פיתות', 'brand': 'אנג\'ל', 'size': (6, 'יחידות'), 'base_price': 4.20, 'barcode_prefix': '7290013'},
        {'name': 'חלה רגילה', 'brand': 'ברמן', 'size': (450, 'גרם'), 'base_price': 6.50, 'barcode_prefix': '7290014'},
        {'name': 'לחמניות המבורגר', 'brand': 'ברמן', 'size': (4, 'יחידות'), 'base_price': 7.90, 'barcode_prefix': '7290015'},
        {'name': 'קרואסון חמאה', 'brand': 'אנג\'ל', 'size': (4, 'יחידות'), 'base_price': 12.90, 'barcode_prefix': '7290016'},
    ],
    'בשר ודגים': [
        {'name': 'שניצל עוף קפוא', 'brand': 'עוף טוב', 'size': (800, 'גרם'), 'base_price': 32.90, 'barcode_prefix': '7290020'},
        {'name': 'חזה עוף טרי', 'brand': 'עוף טוב', 'size': (1, 'ק״ג'), 'base_price': 35.90, 'barcode_prefix': '7290021'},
        {'name': 'כנפיים עוף טריות', 'brand': 'עוף טוב', 'size': (1, 'ק״ג'), 'base_price': 18.90, 'barcode_prefix': '7290022'},
        {'name': 'קציצות עוף קפואות', 'brand': 'זוגלובק', 'size': (600, 'גרם'), 'base_price': 24.90, 'barcode_prefix': '7290023'},
        {'name': 'נקניקיות מעושנות', 'brand': 'תירוש', 'size': (400, 'גרם'), 'base_price': 16.90, 'barcode_prefix': '7290024'},
        {'name': 'סלמון טרי פילה', 'brand': 'דגי נופית', 'size': (300, 'גרם'), 'base_price': 45.90, 'barcode_prefix': '7290025'},
        {'name': 'טונה בשמן זית', 'brand': 'סטרקיסט', 'size': (160, 'גרם'), 'base_price': 8.90, 'barcode_prefix': '7290026'},
    ],
    'פירות וירקות': [
        {'name': 'עגבניות שרי', 'brand': '', 'size': (250, 'גרם'), 'base_price': 7.90, 'barcode_prefix': '7290030'},
        {'name': 'מלפפונים חיתוך', 'brand': '', 'size': (500, 'גרם'), 'base_price': 6.90, 'barcode_prefix': '7290031'},
        {'name': 'בצל צהוב', 'brand': '', 'size': (1, 'ק״ג'), 'base_price': 4.90, 'barcode_prefix': '7290032'},
        {'name': 'גזר חיתוך', 'brand': '', 'size': (1, 'ק״ג'), 'base_price': 5.90, 'barcode_prefix': '7290033'},
        {'name': 'תפוחי אדמה', 'brand': '', 'size': (2, 'ק״ג'), 'base_price': 8.90, 'barcode_prefix': '7290034'},
        {'name': 'בננות', 'brand': '', 'size': (1, 'ק״ג'), 'base_price': 9.90, 'barcode_prefix': '7290035'},
        {'name': 'תפוחים גרני סמית', 'brand': '', 'size': (1, 'ק״ג'), 'base_price': 12.90, 'barcode_prefix': '7290036'},
        {'name': 'תפוזים לסחיטה', 'brand': '', 'size': (2, 'ק״ג'), 'base_price': 9.90, 'barcode_prefix': '7290037'},
        {'name': 'אבוקדו', 'brand': '', 'size': (2, 'יחידות'), 'base_price': 12.90, 'barcode_prefix': '7290038'},
    ],
    'משקאות': [
        {'name': 'קוקה קולה', 'brand': 'קוקה קולה', 'size': (1.5, 'ליטר'), 'base_price': 6.90, 'barcode_prefix': '7290040'},
        {'name': 'ספרייט', 'brand': 'קוקה קולה', 'size': (1.5, 'ליטר'), 'base_price': 6.90, 'barcode_prefix': '7290041'},
        {'name': 'מים מינרלים', 'brand': 'נביעות הר', 'size': (1.5, 'ליטר'), 'base_price': 2.90, 'barcode_prefix': '7290042'},
        {'name': 'מיץ תפוזים טבעי', 'brand': 'פרימור', 'size': (1, 'ליטר'), 'base_price': 8.90, 'barcode_prefix': '7290043'},
        {'name': 'בירה גולדסטאר', 'brand': 'טמפו', 'size': (330, 'מ״ל'), 'base_price': 4.90, 'barcode_prefix': '7290044'},
        {'name': 'יין אדום יבש', 'brand': 'ברקן', 'size': (750, 'מ״ל'), 'base_price': 35.90, 'barcode_prefix': '7290045'},
        {'name': 'אנרגי דרינק', 'brand': 'רד בול', 'size': (250, 'מ״ל'), 'base_price': 8.90, 'barcode_prefix': '7290046'},
    ],
    'חטיפים וממתקים': [
        {'name': 'במבה אגוזי לוז', 'brand': 'אסם', 'size': (60, 'גרם'), 'base_price': 4.50, 'barcode_prefix': '7290050'},
        {'name': 'ביסלי גריל', 'brand': 'אסם', 'size': (70, 'גרם'), 'base_price': 4.90, 'barcode_prefix': '7290051'},
        {'name': 'תפוצ\'יפס מלוח', 'brand': 'שטראוס', 'size': (50, 'גרם'), 'base_price': 3.90, 'barcode_prefix': '7290052'},
        {'name': 'שוקולד מריר', 'brand': 'עלית', 'size': (100, 'גרם'), 'base_price': 6.90, 'barcode_prefix': '7290053'},
        {'name': 'שוקולד חלב', 'brand': 'עלית', 'size': (100, 'גרם'), 'base_price': 6.90, 'barcode_prefix': '7290054'},
        {'name': 'ופל קרמבו', 'brand': 'שטראוס', 'size': (4, 'יחידות'), 'base_price': 8.90, 'barcode_prefix': '7290055'},
        {'name': 'עוגיות פתי בר', 'brand': 'אסם', 'size': (200, 'גרם'), 'base_price': 7.90, 'barcode_prefix': '7290056'},
        {'name': 'דובונים ג\'לי', 'brand': 'שטראוס', 'size': (80, 'גרם'), 'base_price': 4.90, 'barcode_prefix': '7290057'},
    ],
    'מוצרי יסוד': [
        {'name': 'אורז לבן', 'brand': 'אסם', 'size': (1, 'ק״ג'), 'base_price': 8.90, 'barcode_prefix': '7290060'},
        {'name': 'פסטה ספגטי', 'brand': 'ברילה', 'size': (500, 'גרם'), 'base_price': 5.90, 'barcode_prefix': '7290061'},
        {'name': 'קמח לבן', 'brand': 'אסם', 'size': (1, 'ק״ג'), 'base_price': 4.90, 'barcode_prefix': '7290062'},
        {'name': 'סוכר לבן', 'brand': 'סוגת', 'size': (1, 'ק״ג'), 'base_price': 5.90, 'barcode_prefix': '7290063'},
        {'name': 'שמן חמניות', 'brand': 'עין הבר', 'size': (1, 'ליטר'), 'base_price': 12.90, 'barcode_prefix': '7290064'},
        {'name': 'שמן זית', 'brand': 'חוליו', 'size': (500, 'מ״ל'), 'base_price': 24.90, 'barcode_prefix': '7290065'},
        {'name': 'מלח ים', 'brand': 'כרמל', 'size': (1, 'ק״ג'), 'base_price': 3.90, 'barcode_prefix': '7290066'},
        {'name': 'ביצים גודל L', 'brand': '', 'size': (12, 'יחידות'), 'base_price': 14.90, 'barcode_prefix': '7290067'},
    ],
    'מוצרי ניקיון': [
        {'name': 'אבקת כביסה', 'brand': 'אריאל', 'size': (1.3, 'ק״ג'), 'base_price': 28.90, 'barcode_prefix': '7290070'},
        {'name': 'נוזל כלים', 'brand': 'פיירי', 'size': (750, 'מ״ל'), 'base_price': 9.90, 'barcode_prefix': '7290071'},
        {'name': 'נייר טואלט', 'brand': 'סופטלן', 'size': (24, 'גלילים'), 'base_price': 32.90, 'barcode_prefix': '7290072'},
        {'name': 'מגבות נייר', 'brand': 'סופטלן', 'size': (8, 'גלילים'), 'base_price': 24.90, 'barcode_prefix': '7290073'},
        {'name': 'שמפו לשיער', 'brand': 'הד שולדרס', 'size': (400, 'מ״ל'), 'base_price': 19.90, 'barcode_prefix': '7290074'},
    ]
}

# Israeli supermarket pricing strategies
SUPERMARKET_MODIFIERS = {
    1: {'name': 'רמי לוי', 'modifier': 0.92, 'promo_chance': 0.15},  # 8% cheaper, 15% promo chance
    2: {'name': 'יוחננוף', 'modifier': 1.05, 'promo_chance': 0.12},  # 5% more expensive, 12% promo chance
    3: {'name': 'קרפור', 'modifier': 1.02, 'promo_chance': 0.10}     # 2% more expensive, 10% promo chance
}

def clear_existing_data():
    """Clear existing products but keep supermarkets"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("🗑️  Clearing existing product data...")
        cur.execute("DELETE FROM products")
        conn.commit()
        
        cur.close()
        conn.close()
        print("✅ Existing product data cleared")
        
    except Exception as e:
        print(f"❌ Error clearing data: {e}")
        raise

def generate_hebrew_products():
    """Generate Hebrew products with realistic Israeli pricing"""
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("🏪 Starting Hebrew product generation...")
        
        product_count = 0
        
        # Generate products for each supermarket
        for supermarket_id, supermarket_info in SUPERMARKET_MODIFIERS.items():
            print(f"\n🏬 Generating products for {supermarket_info['name']}...")
            
            # Generate products for each category
            for category, products in HEBREW_PRODUCTS.items():
                for product_template in products:
                    
                    # Calculate price for this supermarket
                    base_price = product_template['base_price']
                    modifier = supermarket_info['modifier']
                    final_price = round(base_price * modifier, 2)
                    
                    # Add some random variation (-10% to +10%)
                    price_variation = random.uniform(0.9, 1.1)
                    final_price = round(final_price * price_variation, 2)
                    
                    # Generate barcode (unique per product per supermarket)
                    base_barcode = product_template['barcode_prefix'] + str(supermarket_id).zfill(3)
                    
                    # Determine if product is on sale
                    is_promo = random.random() < supermarket_info['promo_chance']
                    promo_price = None
                    promo_text = None
                    list_price = final_price
                    
                    if is_promo:
                        # Create promotion (10-30% discount)
                        discount = random.uniform(0.10, 0.30)
                        promo_price = round(final_price * (1 - discount), 2)
                        promo_text = f"מבצע {int(discount*100)}% הנחה!"
                    
                    # Determine stock status (5% chance of out of stock)
                    in_stock = random.random() > 0.05
                    
                    # Generate collection timestamp (within last 24 hours)
                    collected_at = datetime.now() - timedelta(
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59)
                    )
                    
                    # Size formatting
                    size_value, size_unit = product_template['size']
                    
                    # Brand handling
                    brand = product_template['brand'] if product_template['brand'] else None
                    
                    # Generate hash for data integrity
                    raw_data = f"{product_template['name']}{brand}{final_price}{supermarket_id}"
                    raw_hash = hashlib.md5(raw_data.encode()).hexdigest()
                    
                    # Insert product
                    cur.execute("""
                        INSERT INTO products (
                            supermarket_id, barcode, canonical_name, brand, category,
                            size_value, size_unit, price, currency, list_price, promo_price, 
                            promo_text, loyalty_only, in_stock, collected_at, source, raw_hash
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        supermarket_id,
                        base_barcode,
                        product_template['name'],
                        brand,
                        category,
                        size_value,
                        size_unit,
                        final_price,
                        'ILS',
                        list_price,
                        promo_price,
                        promo_text,
                        False,  # loyalty_only
                        in_stock,
                        collected_at,
                        'hebrew_generator',
                        raw_hash
                    ))
                    
                    product_count += 1
                    
                    if product_count % 50 == 0:
                        print(f"  ✨ Generated {product_count} products...")
        
        # Commit all changes
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"\n🎉 Successfully generated {product_count} Hebrew products!")
        print(f"📊 Distribution:")
        for supermarket_id, supermarket_info in SUPERMARKET_MODIFIERS.items():
            products_per_store = len(HEBREW_PRODUCTS) * sum(len(products) for products in HEBREW_PRODUCTS.values())
            print(f"   {supermarket_info['name']}: {products_per_store} products")
        
        return product_count
        
    except Exception as e:
        print(f"❌ Error generating products: {e}")
        raise

def show_sample_data():
    """Show sample of generated data"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("\n📋 Sample Hebrew products:")
        print("=" * 80)
        
        cur.execute("""
            SELECT s.name, p.canonical_name, p.brand, p.price, p.promo_price, p.category
            FROM products p
            JOIN supermarkets s ON p.supermarket_id = s.supermarket_id
            ORDER BY p.canonical_name, s.name
            LIMIT 15
        """)
        
        results = cur.fetchall()
        for row in results:
            supermarket, product_name, brand, price, promo_price, category = row
            brand_str = f" - {brand}" if brand else ""
            promo_str = f" (מבצע: ₪{promo_price})" if promo_price else ""
            print(f"🏪 {supermarket}: {product_name}{brand_str} - ₪{price}{promo_str} [{category}]")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error showing sample data: {e}")

def main():
    """Main function"""
    print("🇮🇱 Hebrew Israeli Supermarket Product Generator")
    print("=" * 60)
    
    try:
        # Clear existing data
        clear_existing_data()
        
        # Generate Hebrew products
        total_products = generate_hebrew_products()
        
        # Show sample data
        show_sample_data()
        
        print(f"\n✨ Generation complete! Created {total_products} Hebrew products")
        print("🛒 Ready for Hebrew shopping queries!")
        
    except Exception as e:
        print(f"\n❌ Generation failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
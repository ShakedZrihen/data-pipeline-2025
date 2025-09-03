const db = require('../config/database');

class Product {
  static async search(filters = {}) {
    try {
      let query = `
        SELECT 
          i.id as product_id,
          i.store_id as supermarket_id,
          i.item_code as barcode,
          i.item_name as canonical_name,
          i.item_brand as brand,
          i.manufacturer_name as category,
          i.quantity as size_value,
          i.unit_of_measure as size_unit,
          i.item_price as price,
          'ILS' as currency,
          d.discounted_price as promo_price,
          d.promotion_description as promo_text,
          true as in_stock,
          i.price_update_date as collected_at
        FROM items i
        LEFT JOIN discounts d ON i.chain_id = d.chain_id 
          AND i.store_id = d.store_id 
          AND i.item_code = d.item_code
        WHERE 1=1
      `;
      
      const params = [];
      let paramIndex = 1;
      
      if (filters.name || filters.q) {
        const searchTerm = filters.name || filters.q;
        query += ` AND i.item_name ILIKE $${paramIndex}`;
        params.push(`%${searchTerm}%`);
        paramIndex++;
      }
      
      if (filters.promo !== undefined) {
        if (filters.promo) {
          query += ` AND d.discounted_price IS NOT NULL`;
        } else {
          query += ` AND d.discounted_price IS NULL`;
        }
      }
      
      if (filters.min_price !== undefined) {
        query += ` AND i.item_price >= $${paramIndex}`;
        params.push(filters.min_price);
        paramIndex++;
      }
      
      if (filters.max_price !== undefined) {
        query += ` AND i.item_price <= $${paramIndex}`;
        params.push(filters.max_price);
        paramIndex++;
      }
      
      if (filters.supermarket_id !== undefined) {
        query += ` AND i.store_id = $${paramIndex}`;
        params.push(filters.supermarket_id);
        paramIndex++;
      }
      
      if (filters.chain_id !== undefined) {
        query += ` AND i.chain_id = $${paramIndex}`;
        params.push(filters.chain_id);
        paramIndex++;
      }
      
      query += ` ORDER BY i.price_update_date DESC, i.item_name`;
      
      const result = await db.query(query, params);
      return result.rows;
    } catch (error) {
      throw new Error(`Error searching products: ${error.message}`);
    }
  }

  static async getByBarcode(barcode) {
    try {
      const query = `
        SELECT 
          i.id as product_id,
          i.store_id as supermarket_id,
          s.chain_name as supermarket_name,
          i.item_name as canonical_name,
          i.item_brand as brand,
          i.manufacturer_name as category,
          i.item_code as barcode,
          i.item_price as price,
          d.discounted_price as promo_price,
          d.promotion_description as promo_text,
          i.quantity as size_value,
          i.unit_of_measure as size_unit,
          true as in_stock,
          CASE 
            WHEN d.discounted_price IS NOT NULL THEN i.item_price - d.discounted_price
            ELSE 0
          END as savings
        FROM items i
        JOIN stores s ON i.chain_id = s.chain_id AND i.store_id = s.store_id
        LEFT JOIN discounts d ON i.chain_id = d.chain_id 
          AND i.store_id = d.store_id 
          AND i.item_code = d.item_code
        WHERE i.item_code = $1
        ORDER BY 
          CASE WHEN d.discounted_price IS NOT NULL THEN d.discounted_price ELSE i.item_price END ASC,
          i.price_update_date DESC
      `;
      
      const result = await db.query(query, [barcode]);
      return result.rows;
    } catch (error) {
      throw new Error(`Error fetching products by barcode: ${error.message}`);
    }
  }

  static async getById(productId) {
    try {
      const query = `
        SELECT 
          i.id as product_id,
          i.store_id as supermarket_id,
          s.chain_name as supermarket_name,
          i.item_code as barcode,
          i.item_name as canonical_name,
          i.item_brand as brand,
          i.manufacturer_name as category,
          i.quantity as size_value,
          i.unit_of_measure as size_unit,
          i.item_price as price,
          'ILS' as currency,
          d.discounted_price as promo_price,
          d.promotion_description as promo_text,
          true as in_stock,
          i.price_update_date as collected_at
        FROM items i
        JOIN stores s ON i.chain_id = s.chain_id AND i.store_id = s.store_id
        LEFT JOIN discounts d ON i.chain_id = d.chain_id 
          AND i.store_id = d.store_id 
          AND i.item_code = d.item_code
        WHERE i.id = $1
      `;
      
      const result = await db.query(query, [productId]);
      return result.rows[0] || null;
    } catch (error) {
      throw new Error(`Error fetching product: ${error.message}`);
    }
  }

  static async getByChain(chainId) {
    try {
      const query = `
        SELECT 
          i.id as product_id,
          i.store_id as supermarket_id,
          s.store_name as supermarket_name,
          i.item_code as barcode,
          i.item_name as canonical_name,
          i.item_brand as brand,
          i.manufacturer_name as category,
          i.quantity as size_value,
          i.unit_of_measure as size_unit,
          i.item_price as price,
          'ILS' as currency,
          d.discounted_price as promo_price,
          d.promotion_description as promo_text,
          true as in_stock,
          i.price_update_date as collected_at
        FROM items i
        JOIN stores s ON i.chain_id = s.chain_id AND i.store_id = s.store_id
        LEFT JOIN discounts d ON i.chain_id = d.chain_id 
          AND i.store_id = d.store_id 
          AND i.item_code = d.item_code
        WHERE i.chain_id = $1
        ORDER BY i.price_update_date DESC, i.item_name
      `;
      
      const result = await db.query(query, [chainId]);
      return result.rows;
    } catch (error) {
      throw new Error(`Error fetching products by chain: ${error.message}`);
    }
  }
}

module.exports = Product;

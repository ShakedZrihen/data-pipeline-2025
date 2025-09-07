const db = require('../config/database');

class Supermarket {
  static async getAll() {
    try {
      const query = `
        SELECT 
          id as supermarket_id,
          chain_name as name,
          sub_chain_name as branch_name,
          city,
          address,
          '' as website,
          created_at
        FROM stores
        ORDER BY chain_name, sub_chain_name
      `;
      const result = await db.query(query);
      return result.rows;
    } catch (error) {
      throw new Error(`Error fetching supermarkets: ${error.message}`);
    }
  }

  static async getById(supermarketId) {
    try {
      const query = `
        SELECT 
          id as supermarket_id,
          chain_name as name,
          sub_chain_name as branch_name,
          city,
          address,
          '' as website,
          created_at
        FROM stores
        WHERE id = $1
      `;
      const result = await db.query(query, [supermarketId]);
      return result.rows[0] || null;
    } catch (error) {
      throw new Error(`Error fetching supermarket: ${error.message}`);
    }
  }

  static async getProducts(supermarketId, search = null) {
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
        WHERE i.store_id = $1
      `;
      
      const params = [supermarketId];
      
      if (search) {
        query += ` AND i.item_name ILIKE $2`;
        params.push(`%${search}%`);
      }
      
      query += ` ORDER BY i.price_update_date DESC, i.item_name`;
      
      const result = await db.query(query, params);
      return result.rows;
    } catch (error) {
      throw new Error(`Error fetching supermarket products: ${error.message}`);
    }
  }

  static async getAllChains() {
    try {
      const query = `
        SELECT DISTINCT 
          chain_id,
          chain_name,
          COUNT(*) as store_count
        FROM stores
        GROUP BY chain_id, chain_name
        ORDER BY chain_name
      `;
      const result = await db.query(query);
      return result.rows;
    } catch (error) {
      throw new Error(`Error fetching chains: ${error.message}`);
    }
  }
}

module.exports = Supermarket;

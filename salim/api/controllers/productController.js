const Product = require('../models/Product');

class ProductController {
  static async searchProducts(req, res) {
    try {
      const filters = {
        name: req.query.name || req.query.q,
        promo: req.query.promo !== undefined ? req.query.promo === 'true' : undefined,
        min_price: req.query.min_price !== undefined ? parseFloat(req.query.min_price) : undefined,
        max_price: req.query.max_price !== undefined ? parseFloat(req.query.max_price) : undefined,
        supermarket_id: req.query.supermarket_id !== undefined ? parseInt(req.query.supermarket_id) : undefined,
        chain_id: req.query.chain_id
      };
      
      if (filters.min_price !== undefined && isNaN(filters.min_price)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'min_price must be a valid number' 
        });
      }
      
      if (filters.max_price !== undefined && isNaN(filters.max_price)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'max_price must be a valid number' 
        });
      }
      
      if (filters.supermarket_id !== undefined && isNaN(filters.supermarket_id)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'supermarket_id must be a valid integer' 
        });
      }
      
      if (filters.min_price !== undefined && filters.max_price !== undefined && 
          filters.min_price > filters.max_price) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'min_price cannot be greater than max_price' 
        });
      }
      
      const products = await Product.search(filters);
      res.json(products);
    } catch (error) {
      console.error('Error in searchProducts:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getProductsByBarcode(req, res) {
    try {
      const { barcode } = req.params;
      
      if (!barcode || barcode.trim() === '') {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'barcode is required' 
        });
      }
      
      const products = await Product.getByBarcode(barcode.trim());
      
      if (products.length === 0) {
        return res.status(404).json({ 
          error: 'Not found',
          message: 'No products found with this barcode' 
        });
      }
      
      res.json(products);
    } catch (error) {
      console.error('Error in getProductsByBarcode:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getProductById(req, res) {
    try {
      const { product_id } = req.params;
      const productId = parseInt(product_id);
      
      if (isNaN(productId)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'product_id must be a valid integer' 
        });
      }
      
      const product = await Product.getById(productId);
      
      if (!product) {
        return res.status(404).json({ 
          error: 'Not found',
          message: 'Product not found' 
        });
      }
      
      res.json(product);
    } catch (error) {
      console.error('Error in getProductById:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getProductsByChain(req, res) {
    try {
      const { chain_id } = req.params;
      
      if (!chain_id || chain_id.trim() === '') {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'chain_id is required' 
        });
      }
      
      const products = await Product.getByChain(chain_id.trim());
      res.json(products);
    } catch (error) {
      console.error('Error in getProductsByChain:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }
}

module.exports = ProductController;

const Supermarket = require('../models/Supermarket');

class SupermarketController {
  static async getAllSupermarkets(req, res) {
    try {
      const supermarkets = await Supermarket.getAll();
      res.json(supermarkets);
    } catch (error) {
      console.error('Error in getAllSupermarkets:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getAllChains(req, res) {
    try {
      const chains = await Supermarket.getAllChains();
      res.json(chains);
    } catch (error) {
      console.error('Error in getAllChains:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getSupermarketById(req, res) {
    try {
      const { supermarket_id } = req.params;
      const supermarketId = parseInt(supermarket_id);
      
      if (isNaN(supermarketId)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'supermarket_id must be a valid integer' 
        });
      }
      
      const supermarket = await Supermarket.getById(supermarketId);
      
      if (!supermarket) {
        return res.status(404).json({ 
          error: 'Not found',
          message: 'Supermarket not found' 
        });
      }
      
      res.json(supermarket);
    } catch (error) {
      console.error('Error in getSupermarketById:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }

  static async getSupermarketProducts(req, res) {
    try {
      const { supermarket_id } = req.params;
      const { search } = req.query;
      const supermarketId = parseInt(supermarket_id);
      
      if (isNaN(supermarketId)) {
        return res.status(400).json({ 
          error: 'Bad request',
          message: 'supermarket_id must be a valid integer' 
        });
      }
      
      const supermarket = await Supermarket.getById(supermarketId);
      if (!supermarket) {
        return res.status(404).json({ 
          error: 'Not found',
          message: 'Supermarket not found' 
        });
      }
      
      const products = await Supermarket.getProducts(supermarketId, search);
      res.json(products);
    } catch (error) {
      console.error('Error in getSupermarketProducts:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        message: error.message 
      });
    }
  }
}

module.exports = SupermarketController;

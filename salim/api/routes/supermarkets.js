const express = require('express');
const SupermarketController = require('../controllers/supermarketController');

const router = express.Router();

/**
 * @swagger
 * tags:
 *   name: supermarkets
 *   description: Supermarket management endpoints
 */

/**
 * @swagger
 * /supermarkets:
 *   get:
 *     summary: Get all supermarkets (stores)
 *     tags: [supermarkets]
 *     responses:
 *       200:
 *         description: List of all supermarkets
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/SupermarketResponse'
 *       500:
 *         description: Internal server error
 */
router.get('/', SupermarketController.getAllSupermarkets);

/**
 * @swagger
 * /supermarkets/chains:
 *   get:
 *     summary: Get all unique supermarket chains
 *     tags: [supermarkets]
 *     responses:
 *       200:
 *         description: List of all supermarket chains
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   chain_id:
 *                     type: string
 *                   chain_name:
 *                     type: string
 *                   store_count:
 *                     type: integer
 *       500:
 *         description: Internal server error
 */
router.get('/chains', SupermarketController.getAllChains);

/**
 * @swagger
 * /supermarkets/{supermarket_id}:
 *   get:
 *     summary: Get a specific supermarket by ID
 *     tags: [supermarkets]
 *     parameters:
 *       - in: path
 *         name: supermarket_id
 *         required: true
 *         schema:
 *           type: integer
 *         description: The supermarket ID
 *     responses:
 *       200:
 *         description: Supermarket details
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/SupermarketResponse'
 *       400:
 *         description: Bad request - invalid supermarket_id
 *       404:
 *         description: Supermarket not found
 *       500:
 *         description: Internal server error
 */
router.get('/:supermarket_id', SupermarketController.getSupermarketById);

/**
 * @swagger
 * /supermarkets/{supermarket_id}/products:
 *   get:
 *     summary: Get products from a specific supermarket
 *     tags: [supermarkets]
 *     parameters:
 *       - in: path
 *         name: supermarket_id
 *         required: true
 *         schema:
 *           type: integer
 *         description: The supermarket ID
 *       - in: query
 *         name: search
 *         schema:
 *           type: string
 *         description: Search in product names
 *     responses:
 *       200:
 *         description: List of products from the supermarket
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ProductResponse'
 *       400:
 *         description: Bad request - invalid supermarket_id
 *       404:
 *         description: Supermarket not found
 *       500:
 *         description: Internal server error
 */
router.get('/:supermarket_id/products', SupermarketController.getSupermarketProducts);

module.exports = router;

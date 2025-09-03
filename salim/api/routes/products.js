const express = require('express');
const ProductController = require('../controllers/productController');

const router = express.Router();

/**
 * @swagger
 * tags:
 *   name: products
 *   description: Product management endpoints
 */

/**
 * @swagger
 * /products:
 *   get:
 *     summary: Search products with various filters
 *     tags: [products]
 *     parameters:
 *       - in: query
 *         name: name
 *         schema:
 *           type: string
 *         description: Filter by product name (alias for 'q')
 *       - in: query
 *         name: q
 *         schema:
 *           type: string
 *         description: Filter by product name (alias for 'name')
 *       - in: query
 *         name: promo
 *         schema:
 *           type: boolean
 *         description: Filter by promotion status (true=on sale, false=regular price)
 *       - in: query
 *         name: min_price
 *         schema:
 *           type: number
 *         description: Minimum price filter in ILS
 *       - in: query
 *         name: max_price
 *         schema:
 *           type: number
 *         description: Maximum price filter in ILS
 *       - in: query
 *         name: supermarket_id
 *         schema:
 *           type: integer
 *         description: Filter by specific supermarket ID
 *       - in: query
 *         name: chain_id
 *         schema:
 *           type: string
 *         description: Filter by specific supermarket chain ID
 *       - in: query
 *         name: brand
 *         schema:
 *           type: string
 *         description: Filter by product brand
 *       - in: query
 *         name: category
 *         schema:
 *           type: string
 *         description: Filter by product category
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           minimum: 1
 *           maximum: 1000
 *           default: 100
 *         description: Maximum number of results to return
 *       - in: query
 *         name: offset
 *         schema:
 *           type: integer
 *           minimum: 0
 *           default: 0
 *         description: Number of results to skip for pagination
 *       - in: query
 *         name: sort_by
 *         schema:
 *           type: string
 *           enum: [price, name, brand, category, date]
 *           default: date
 *         description: Field to sort results by
 *       - in: query
 *         name: sort_order
 *         schema:
 *           type: string
 *           enum: [asc, desc]
 *           default: desc
 *         description: Sort order (ascending or descending)
 *     responses:
 *       200:
 *         description: List of products matching the filters
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ProductResponse'
 *       400:
 *         description: Bad request - invalid parameters
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ErrorResponse'
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ErrorResponse'
 */
router.get('/', ProductController.searchProducts);

/**
 * @swagger
 * /products/chain/{chain_id}:
 *   get:
 *     summary: Get all products from a specific supermarket chain
 *     tags: [products]
 *     parameters:
 *       - in: path
 *         name: chain_id
 *         required: true
 *         schema:
 *           type: string
 *         description: The supermarket chain ID
 *     responses:
 *       200:
 *         description: List of products from the chain
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ProductResponse'
 *       400:
 *         description: Bad request - chain_id is required
 *       500:
 *         description: Internal server error
 */
router.get('/chain/:chain_id', ProductController.getProductsByChain);

/**
 * @swagger
 * /products/barcode/{barcode}:
 *   get:
 *     summary: Get all products with the same barcode across different supermarkets, sorted by price
 *     tags: [products]
 *     parameters:
 *       - in: path
 *         name: barcode
 *         required: true
 *         schema:
 *           type: string
 *         description: The product barcode
 *     responses:
 *       200:
 *         description: List of products with the same barcode across supermarkets
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/PriceComparisonResponse'
 *       400:
 *         description: Bad request - barcode is required
 *       404:
 *         description: No products found with this barcode
 *       500:
 *         description: Internal server error
 */
router.get('/barcode/:barcode', ProductController.getProductsByBarcode);

/**
 * @swagger
 * /products/{product_id}:
 *   get:
 *     summary: Get a specific product by ID
 *     tags: [products]
 *     parameters:
 *       - in: path
 *         name: product_id
 *         required: true
 *         schema:
 *           type: integer
 *         description: The product ID
 *     responses:
 *       200:
 *         description: Product details
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ProductResponse'
 *       400:
 *         description: Bad request - invalid product_id
 *       404:
 *         description: Product not found
 *       500:
 *         description: Internal server error
 */
router.get('/:product_id', ProductController.getProductById);

module.exports = router;

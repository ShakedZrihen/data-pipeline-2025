/**
 * @swagger
 * components:
 *   schemas:
 *     ProductResponse:
 *       type: object
 *       properties:
 *         product_id:
 *           type: integer
 *           description: Unique product identifier
 *           example: 12345
 *         supermarket_id:
 *           type: integer
 *           description: Supermarket store ID
 *           example: 1
 *         barcode:
 *           type: string
 *           description: Product barcode
 *           example: "7290000000000"
 *         canonical_name:
 *           type: string
 *           description: Product name
 *           example: "Milk 3% 1L"
 *         brand:
 *           type: string
 *           description: Product brand
 *           example: "Tnuva"
 *         category:
 *           type: string
 *           description: Product category
 *           example: "Dairy"
 *         price:
 *           type: number
 *           format: float
 *           description: Regular price in ILS
 *           example: 5.99
 *         promo_price:
 *           type: number
 *           format: float
 *           nullable: true
 *           description: Promotional price if available
 *           example: 4.99
 *         promo_text:
 *           type: string
 *           nullable: true
 *           description: Promotion description
 *           example: "Buy 2 for ₪8.99"
 *         size_value:
 *           type: number
 *           format: float
 *           description: Product size value
 *           example: 1.0
 *         size_unit:
 *           type: string
 *           description: Unit of measurement
 *           example: "L"
 *         in_stock:
 *           type: boolean
 *           description: Product availability
 *           example: true
 *         collected_at:
 *           type: string
 *           format: date-time
 *           description: Last update timestamp
 *           example: "2024-01-15T10:30:00Z"
 *         currency:
 *           type: string
 *           description: Currency code
 *           example: "ILS"
 *       required:
 *         - product_id
 *         - supermarket_id
 *         - barcode
 *         - canonical_name
 *         - price
 *       example:
 *         product_id: 12345
 *         supermarket_id: 1
 *         barcode: "7290000000000"
 *         canonical_name: "Milk 3% 1L"
 *         brand: "Tnuva"
 *         category: "Dairy"
 *         price: 5.99
 *         promo_price: 4.99
 *         promo_text: "Buy 2 for ₪8.99"
 *         size_value: 1.0
 *         size_unit: "L"
 *         in_stock: true
 *         collected_at: "2024-01-15T10:30:00Z"
 *         currency: "ILS"
 *     
 *     SupermarketResponse:
 *       type: object
 *       properties:
 *         supermarket_id:
 *           type: integer
 *           description: Unique supermarket identifier
 *           example: 1
 *         chain_id:
 *           type: string
 *           description: Chain identifier
 *           example: "7290700100008"
 *         chain_name:
 *           type: string
 *           description: Name of the supermarket chain
 *           example: "Rami Levi"
 *         store_name:
 *           type: string
 *           description: Individual store name
 *           example: "Rami Levi - Tel Aviv Center"
 *         address:
 *           type: string
 *           description: Store address
 *           example: "123 Dizengoff St, Tel Aviv"
 *         city:
 *           type: string
 *           description: City where store is located
 *           example: "Tel Aviv"
 *         phone:
 *           type: string
 *           nullable: true
 *           description: Store phone number
 *           example: "+972-3-1234567"
 *         email:
 *           type: string
 *           nullable: true
 *           description: Store email
 *           example: "telaviv@ramilevi.co.il"
 *       required:
 *         - supermarket_id
 *         - chain_id
 *         - chain_name
 *         - store_name
 *       example:
 *         supermarket_id: 1
 *         chain_id: "7290700100008"
 *         chain_name: "Rami Levi"
 *         store_name: "Rami Levi - Tel Aviv Center"
 *         address: "123 Dizengoff St, Tel Aviv"
 *         city: "Tel Aviv"
 *         phone: "+972-3-1234567"
 *         email: "telaviv@ramilevi.co.il"
 *     
 *     PriceComparisonResponse:
 *       type: object
 *       properties:
 *         product_id:
 *           type: integer
 *           description: Unique product identifier
 *           example: 12345
 *         supermarket_id:
 *           type: integer
 *           description: Supermarket store ID
 *           example: 1
 *         supermarket_name:
 *           type: string
 *           description: Name of the supermarket
 *           example: "Rami Levi - Tel Aviv Center"
 *         canonical_name:
 *           type: string
 *           description: Product name
 *           example: "Milk 3% 1L"
 *         brand:
 *           type: string
 *           description: Product brand
 *           example: "Tnuva"
 *         category:
 *           type: string
 *           description: Product category
 *           example: "Dairy"
 *         barcode:
 *           type: string
 *           description: Product barcode
 *           example: "7290000000000"
 *         price:
 *           type: number
 *           format: float
 *           description: Regular price in ILS
 *           example: 5.99
 *         promo_price:
 *           type: number
 *           format: float
 *           nullable: true
 *           description: Promotional price if available
 *           example: 4.99
 *         promo_text:
 *           type: string
 *           nullable: true
 *           description: Promotion description
 *           example: "Buy 2 for ₪8.99"
 *         size_value:
 *           type: number
 *           format: float
 *           description: Product size value
 *           example: 1.0
 *         size_unit:
 *           type: string
 *           description: Unit of measurement
 *           example: "L"
 *         in_stock:
 *           type: boolean
 *           description: Product availability
 *           example: true
 *         savings:
 *           type: number
 *           format: float
 *           nullable: true
 *           description: Amount saved with promotion
 *           example: 1.00
 *       required:
 *         - product_id
 *         - supermarket_id
 *         - supermarket_name
 *         - canonical_name
 *         - barcode
 *         - price
 *       example:
 *         product_id: 12345
 *         supermarket_id: 1
 *         supermarket_name: "Rami Levi - Tel Aviv Center"
 *         canonical_name: "Milk 3% 1L"
 *         brand: "Tnuva"
 *         category: "Dairy"
 *         barcode: "7290000000000"
 *         price: 5.99
 *         promo_price: 4.99
 *         promo_text: "Buy 2 for ₪8.99"
 *         size_value: 1.0
 *         size_unit: "L"
 *         in_stock: true
 *         savings: 1.00
 *     
 *     ChainResponse:
 *       type: object
 *       properties:
 *         chain_id:
 *           type: string
 *           description: Unique chain identifier
 *           example: "7290700100008"
 *         chain_name:
 *           type: string
 *           description: Name of the supermarket chain
 *           example: "Rami Levi"
 *         store_count:
 *           type: integer
 *           description: Number of stores in this chain
 *           example: 45
 *         total_products:
 *           type: integer
 *           description: Total number of products across all stores
 *           example: 15000
 *         avg_price:
 *           type: number
 *           format: float
 *           description: Average product price in this chain
 *           example: 12.45
 *       required:
 *         - chain_id
 *         - chain_name
 *         - store_count
 *       example:
 *         chain_id: "7290700100008"
 *         chain_name: "Rami Levi"
 *         store_count: 45
 *         total_products: 15000
 *         avg_price: 12.45
 *     
 *     ErrorResponse:
 *       type: object
 *       properties:
 *         error:
 *           type: string
 *           description: Error type
 *           example: "Bad Request"
 *         message:
 *           type: string
 *           description: Detailed error message
 *           example: "Invalid parameter value"
 *         timestamp:
 *           type: string
 *           format: date-time
 *           description: When the error occurred
 *           example: "2024-01-15T10:30:00Z"
 *       required:
 *         - error
 *         - message
 *       example:
 *         error: "Bad Request"
 *         message: "Invalid parameter value"
 *         timestamp: "2024-01-15T10:30:00Z"
 *     
 *     SuccessResponse:
 *       type: object
 *       properties:
 *         status:
 *           type: string
 *           description: Response status
 *           example: "OK"
 *         message:
 *           type: string
 *           description: Success message
 *           example: "Operation completed successfully"
 *         timestamp:
 *           type: string
 *           format: date-time
 *           description: When the operation completed
 *           example: "2024-01-15T10:30:00Z"
 *       required:
 *         - status
 *         - message
 *       example:
 *         status: "OK"
 *         message: "Operation completed successfully"
 *         timestamp: "2024-01-15T10:30:00Z"
 */

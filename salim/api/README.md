# Supermarket Product API

A Node.js REST API for querying supermarket product prices with filtering, searching, and comparison capabilities. **Compatible with the database schema from `create_tables.py`**.

##Features

- **Supermarket Management**: Get all supermarkets (stores) and their details
- **Chain Management**: Get unique supermarket chains and their store counts
- **Product Search**: Advanced filtering by name, price, promotion status, supermarket, and chain
- **Price Comparison**: Compare product prices across different supermarkets by barcode
- **Promotion Support**: Integrated discount and promotion information
- **RESTful Design**: Clean, intuitive API endpoints
- **Security**: Rate limiting, CORS, and security headers
- **Error Handling**: Comprehensive error handling and validation

##API Endpoints

###Supermarkets Routes (`/api/supermarkets`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/supermarkets` | Get all supermarkets (stores) |
| GET | `/supermarkets/chains` | Get all unique supermarket chains |
| GET | `/supermarkets/{supermarket_id}` | Get a specific supermarket by ID |
| GET | `/supermarkets/{supermarket_id}/products` | Get products from a specific supermarket |

### Products Routes (`/api/products`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/products` | Search products with various filters |
| GET | `/products/chain/{chain_id}` | Get all products from a specific chain |
| GET | `/products/barcode/{barcode}` | Get products by barcode across supermarkets |
| GET | `/products/{product_id}` | Get a specific product by ID |

## Database Schema Compatibility

This API is designed to work with the database schema created by `create_tables.py`:

### **Tables Used:**
- **`stores`** - Supermarket store information (chain_id, chain_name, store_name, address, city)
- **`items`** - Product information (item_code, item_name, item_price, manufacturer_name, item_brand)
- **`discounts`** - Promotion and discount information (discounted_price, promotion_description)

### **Key Mappings:**
- `stores.id` → `supermarket_id`
- `stores.chain_name` → `supermarket_name`
- `stores.sub_chain_name` → `branch_name`
- `items.item_code` → `barcode`
- `items.item_name` → `canonical_name`
- `items.item_price` → `price`
- `discounts.discounted_price` → `promo_price`

## Installation & Setup

### Prerequisites

- Node.js (v14 or higher)
- PostgreSQL database with tables from `create_tables.py`
- npm or yarn

### 1. Clone and Install Dependencies

```bash
cd salim/api
npm install
```

### 2. Environment Configuration

Copy the environment file and configure your database:

```bash
copy env.example .env
```

Edit `.env` with your database credentials:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Server Configuration
PORT=3001
NODE_ENV=development

# Security
RATE_LIMIT_WINDOW_MS=900000
RATE_LIMIT_MAX_REQUESTS=100
```

### 3. Database Setup

Ensure your PostgreSQL database has the required tables by running `create_tables.py`:

```bash
cd ../Project/enricher
python create_tables.py
```

The script will create:
- `stores` table for supermarket information
- `items` table for product information  
- `discounts` table for promotion information
- `processed_files` table for tracking processed files

### 4. Start the Server

```bash
cd ../../api
# Development mode with auto-reload
npm run dev

# Production mode
npm start
```

The API will be available at `http://localhost:3001`

## 📖 API Usage Examples

### Get All Supermarkets

```bash
curl http://localhost:3001/api/supermarkets
```

### Get All Chains

```bash
curl http://localhost:3001/api/supermarkets/chains
```

### Search Products

```bash
# Search by name
curl "http://localhost:3001/api/products?name=חלב"

# Filter by price range
curl "http://localhost:3001/api/products?min_price=5&max_price=20"

# Filter by promotion status
curl "http://localhost:3001/api/products?promo=true"

# Filter by supermarket
curl "http://localhost:3001/api/products?supermarket_id=1"

# Filter by chain
curl "http://localhost:3001/api/products?chain_id=rami_levi"
```

### Get Products by Chain

```bash
curl http://localhost:3001/api/products/chain/rami_levi
```

### Compare Prices by Barcode

```bash
curl http://localhost:3001/api/products/barcode/7290000000001
```

### Get Products from Specific Supermarket

```bash
curl "http://localhost:3001/api/supermarkets/1/products?search=חלב"
```

## Development

### Project Structure

```
api/
├── config/
│   └── database.js          # Database configuration
├── controllers/
│   ├── supermarketController.js  # Supermarket business logic
│   └── productController.js      # Product business logic
├── middleware/
│   ├── errorHandler.js      # Global error handling
│   └── rateLimiter.js       # Rate limiting
├── models/
│   ├── Supermarket.js       # Supermarket data model (stores table)
│   └── Product.js           # Product data model (items + discounts tables)
├── routes/
│   ├── supermarkets.js      # Supermarket routes
│   └── products.js          # Product routes
├── server.js                # Main server file
├── package.json             # Dependencies and scripts
└── README.md               # This file
```

### Available Scripts

```bash
npm start          # Start production server
npm run dev        # Start development server with nodemon
npm test           # Run tests (when implemented)
```

## Error Handling

The API returns consistent error responses:

```json
{
  "error": "Error Type",
  "message": "Human-readable error message"
}
```

Common HTTP status codes:
- `200` - Success
- `400` - Bad Request (validation errors)
- `404` - Not Found
- `429` - Too Many Requests (rate limited)
- `500` - Internal Server Error

## Security Features

- **Rate Limiting**: Configurable request limits per IP
- **CORS**: Cross-origin resource sharing configuration
- **Helmet**: Security headers for Express
- **Input Validation**: Parameter validation and sanitization
- **SQL Injection Protection**: Parameterized queries

## Performance

- **Connection Pooling**: Efficient database connection management
- **Indexed Queries**: Database indexes for fast searches
- **Rate Limiting**: Prevents abuse and ensures fair usage
- **Optimized JOINs**: Efficient queries across stores, items, and discounts tables

## Data Flow

1. **Stores** table contains supermarket chain and store information
2. **Items** table contains product pricing and details
3. **Discounts** table contains promotional pricing information
4. API joins these tables to provide comprehensive product information
5. Supports filtering by store, chain, price, promotions, and search terms
## Contributing

1. Follow the existing code structure
2. Add proper error handling
3. Include input validation
4. Update documentation for new endpoints
5. Test thoroughly before submitting

## License

MIT License - see LICENSE file for details

## Support

For issues and questions:
1. Check the error logs
2. Verify database connectivity
3. Ensure environment variables are set correctly
4. Verify tables exist and match the expected schema from `create_tables.py`
5. Check that the database has data in the stores, items, and discounts tables

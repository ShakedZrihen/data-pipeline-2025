# Supermarket Product API

A Node.js REST API for querying supermarket product prices with filtering, searching, and comparison capabilities.

## Features

- Supermarket Management: Get all supermarkets and their details
- Chain Management: Get unique supermarket chains and their store counts
- Product Search: Advanced filtering by name, price, promotion status, supermarket, and chain
- Price Comparison: Compare product prices across different supermarkets by barcode
- Promotion Support: Integrated discount and promotion information
- RESTful Design: Clean, intuitive API endpoints

## API Endpoints

### Supermarkets Routes (`/api/supermarkets`)

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
```

### 3. Start the Server

```bash
# Development mode with auto-reload
npm run dev

# Production mode
npm start
```

The API will be available at `http://localhost:3001`

## API Usage Examples

### Get All Supermarkets

```bash
curl http://localhost:3001/api/supermarkets
```

### Search Products

```bash
# Search by name
curl "http://localhost:3001/api/products?name=חלב"

# Filter by price range
curl "http://localhost:3001/api/products?min_price=5&max_price=20"

# Filter by promotion status
curl "http://localhost:3001/api/products?promo=true"
```

### Compare Prices by Barcode

```bash
curl http://localhost:3001/api/products/barcode/7290000000001
```

## Available Scripts

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
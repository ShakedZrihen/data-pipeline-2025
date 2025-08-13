# Product Search MCP Server

An MCP (Model Context Protocol) server that provides three tools for product search and price comparison with Hebrew text support.

## Tools

### 1. search_product(product_name: str)
Searches for products by name and returns a list of matching products with Hebrew text.

**Example Response:**
```json
[
  {
    "value": "חלב תנובה טרי 3% בקרטון, כשרות מהדרין, 1 ליטר",
    "label": "חלב תנובה טרי 3% בקרטון, כשרות מהדרין, 1 ליטר",
    "id": "7290027600007_7290004131074",
    "parts": {
      "name_and_contents": "חלב תנובה טרי 3% בקרטון, כשרות מהדרין, 1 ליטר",
      "manufacturer_and_barcode": "יצרן/מותג: תנובה, ברקוד: 7290004131074",
      "pack_size": "",
      "small_image": "base64_image_data",
      "price_range": ["7.20", "7.28"],
      "chainnames": ""
    }
  }
]
```

### 2. compare_results(product_id: str, shopping_address: str = "כפר סבא")
Returns HTML price comparison table for a specific product ID and shopping address.

**Parameters:**
- `product_id`: The product barcode/ID from search_product results
- `shopping_address`: Shopping location (optional, defaults to "כפר סבא")

**Returns:** Complete HTML page with price comparison table from different stores and chains.

### 3. find_best_basket(products: str[])
Finds the cheapest baskets by רשת (chain) for a list of product IDs.

**Example Response:**
```json
[
  {
    "רשת": "רמי לוי",
    "שם החנות": "כפר סבא",
    "כתובת": "גלגלי הפלדה 5, כפר סבא",
    "מחיר כולל לסל": "11.70"
  }
]
```

## Installation

```bash
npm install
```

## Usage

### Start the MCP server
```bash
node server.js
```

### Test the server
```bash
node test-server.js
```

## Configuration

- **search_product**: Uses the real API at `https://chp.co.il/autocompletion/product_extended?term=<product_name>`
- **compare_results**: ✅ **Now uses real API**: `https://chp.co.il/main_page/compare_results?shopping_address=<address>&product_barcode=<product_id>`
- **find_best_basket**: Currently uses mock data for demonstration. In production, this would connect to real pricing APIs.

## Features

- **Real API Integration**: 
  - Product search: `https://chp.co.il/autocompletion/product_extended?term=<product_name>`
  - Price comparison: `https://chp.co.il/main_page/compare_results?shopping_address=<address>&product_barcode=<product_id>`
- Hebrew text support with full Unicode handling
- Product search with real-time results from Israeli supermarket chains
- **Live price comparison** across multiple chains with real store data
- Best basket calculation to find cheapest shopping options
- HTML formatted results for price comparison
- Fallback to mock data if API is unavailable
- Error handling and graceful degradation
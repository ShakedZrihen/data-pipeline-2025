# Salim API Application

FastAPI-based REST API for querying supermarket product data, deployed on AWS Lambda with API Gateway integration. This application serves as the client-facing interface to access processed supermarket data stored in Supabase.

## 🚀 Live API

**Base URL**: `https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod`

**Interactive Documentation**: https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/docs

## Project Structure

```
salim/app/
├── main.py              # FastAPI application entry point
├── lambda_function.py   # AWS Lambda adapter 
├── models.py           # Pydantic response models
├── database.py         # Supabase database service
├── requirements.txt    # Python dependencies
└── routes/
    ├── __init__.py
    └── api/
        ├── __init__.py      # API router setup
        ├── health.py        # Health check endpoints
        ├── supermarkets.py  # Supermarket endpoints
        └── products.py      # Product endpoints
```

## API Endpoints

### 🏪 Supermarkets Routes

- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/supermarkets`** - Get all supermarkets
- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/supermarkets/{supermarket_id}`** - Get specific supermarket by ID  
- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/supermarkets/{supermarket_id}/products`** - Get products from supermarket

**Example:**
```bash
curl https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/supermarkets
curl https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/supermarkets/1/products
```

### 🛒 Products Routes

- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/products`** - Search products with filters:
  - `name` - Filter by product name
  - `promo` - Filter by promotion status (true/false)
  - `min_price` - Minimum price filter
  - `max_price` - Maximum price filter  
  - `supermarket_id` - Filter by specific supermarket

**Examples:**
```bash
curl "https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/products?name=milk"
curl "https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/products?promo=true&min_price=5"
```

### 🏥 Health Routes

- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/health/`** - Basic health check
- **`GET https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/health/detailed`** - Detailed health check

**Examples:**
```bash
curl https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/health/
curl https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod/api/v1/health/detailed
```

## Local Development

### Prerequisites
- Python 3.11+
- Access to Supabase database

### Setup

```bash
# Navigate to app directory
cd salim/app

# Install dependencies  
pip install -r requirements.txt

# Set environment variables
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"

# Run development server
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### API Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 🚀 AWS Deployment (Production)

### Current Deployment Status: ✅ LIVE
- **AWS Lambda Function**: `supermarket-api`
- **API Gateway**: `https://olqgdfdgye.execute-api.eu-west-1.amazonaws.com/prod`
- **Region**: `eu-west-1`
- **Runtime**: Python 3.11

### 1. Create Deployment Package

```bash
# Install dependencies using Docker (from app directory)
docker run --rm -v "C:\Users\HaDzE7\Desktop\University SE\data-pipeline-2025\salim\app:/var/task" python:3.11 pip install -r /var/task/requirements.txt mangum -t /var/task

# Create deployment zip
powershell -Command "Compress-Archive -Path * -DestinationPath 'salim-api-lambda-final.zip' -Force"
```

### 2. AWS Lambda Configuration

**Current Settings:**
- **Function name**: `supermarket-api`
- **Runtime**: Python 3.11
- **Handler**: `lambda_function.lambda_handler`  
- **Timeout**: 30 seconds
- **Memory**: 512 MB
- **Architecture**: x86_64

**Environment Variables Required:**
```
SUPABASE_URL = https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY = your-service-role-key
```

### 3. API Gateway Integration

**Current Setup:**
- **API Name**: `supermarket-api-v2`
- **Type**: REST API
- **Stage**: `prod`
- **Endpoint**: Regional
- **CORS**: Enabled

**Resource Configuration:**
```
/                    (GET method for root endpoint)
/{proxy+}            (ANY method with Lambda Proxy integration)
```

**Lambda Integration:**
- **Integration Type**: Lambda Function  
- **Lambda Proxy Integration**: ✅ Enabled
- **Lambda Function**: `arn:aws:lambda:eu-west-1:975050127428:function:supermarket-api`
- **Permissions**: Auto-granted by API Gateway

## Response Examples

### Supermarket Response
```json
{
  "supermarket_id": 1,
  "name": "Rami Levi",
  "branch_name": "Tel Aviv Branch",
  "city": null,
  "address": null,
  "website": null,
  "created_at": "2025-09-05T10:00:00+00:00"
}
```

### Product Response
```json
{
  "id": 123,
  "provider": "Rami Levi", 
  "branch": "Tel Aviv Branch",
  "product_name": "חלב 3% 1 ליטר",
  "manufacturer": "Tnuva",
  "price": 6.90,
  "unit": "liter",
  "category": "Dairy",
  "is_promotion": false,
  "is_kosher": true,
  "file_timestamp": "2025-09-05T08:00:00+00:00",
  "created_at": "2025-09-05T10:00:00+00:00"
}
```

## Database Schema

The API queries the `supermarket_data` table with these key fields:
- `provider` - Supermarket chain name
- `branch` - Specific branch location  
- `product_name` - Product name (Hebrew/original)
- `manufacturer` - Brand/company name
- `price` - Product price
- `category` - Product category (enriched by OpenAI)
- `is_promotion` - Promotion status
- `is_kosher` - Kosher certification (enriched by OpenAI)

## Performance & Limitations

- **Query Limit**: 1000 items per request
- **Timeout**: 30 seconds for Lambda  
- **Rate Limiting**: Configure at API Gateway level
- **Caching**: Consider adding for frequently accessed data

## Monitoring

- **CloudWatch Logs**: Monitor Lambda execution
- **API Gateway Metrics**: Track request volume and latency
- **Health Endpoint**: Use for uptime monitoring

## Security

- **CORS**: Configured for all origins (adjust for production)
- **Authentication**: Consider adding API key auth
- **Input Validation**: Automatic via Pydantic models
- **Environment Variables**: Secure storage of database credentials

## Troubleshooting

1. **Database Connection Issues**:
   - Verify environment variables
   - Check Supabase service status
   - Ensure Lambda has internet access

2. **Import Errors**:
   - Verify all dependencies in deployment package
   - Check Python version compatibility

3. **CORS Issues**:
   - Verify API Gateway CORS configuration
   - Check preflight request handling
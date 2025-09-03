const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Supermarket Product API',
      version: '1.0.0',
      description: `
        ## 🛒 Israeli Supermarket Price Comparison API
        
        Compare prices across major Israeli supermarkets including:
        - **Rami Levi** - Generally offering competitive prices
        - **Yohananof** - Premium supermarket chain
        - **Carrefour** - International retail chain
        
        ### 🔍 Key Features:
        - **Product Search** - Find products by name, category, or brand
        - **Price Comparison** - Compare same products across different stores
        - **Barcode Lookup** - Scan barcodes to find products instantly
        - **Promotion Filtering** - Find products on sale
        - **Live Data** - Real-time pricing information
        
        ### 📊 Sample Data:
        - **3,000+ products** across all categories
        - **12.2% products on sale** with promotional pricing
        - **Foundation products** like milk, bread, eggs available in all stores
        
        ### 🏷️ Price Examples:
        - Milk 1L: ₪5.34 (Rami Levi) vs ₪6.03 (Carrefour)
        - White Bread: ₪4.21 (Rami Levi) vs ₪4.81 (Yohananof)
      `,
      contact: {
        name: 'API Support',
        email: 'support@example.com'
      },
      license: {
        name: 'MIT',
        url: 'https://opensource.org/licenses/MIT'
      }
    },
    servers: [
      {
        url: 'http://localhost:3001/api',
        description: 'Development server'
      },
      {
        url: 'https://your-production-domain.com/api',
        description: 'Production server'
      }
    ],
    tags: [
      {
        name: 'supermarkets',
        description: 'Supermarket management endpoints - Get store information and their product catalogs'
      },
      {
        name: 'products',
        description: 'Product search, lookup, and price comparison operations - Find products by various criteria and compare prices across supermarkets'
      },
      {
        name: 'analytics',
        description: 'Analytics and reporting endpoints - Get insights into pricing trends, promotions, and chain performance'
      },
      {
        name: 'users',
        description: 'User management and authentication - Register, login, manage profiles, favorites, and shopping lists'
      }
    ],
    components: {
      securitySchemes: {
        ApiKeyAuth: {
          type: 'apiKey',
          in: 'header',
          name: 'X-API-Key'
        }
      }
    }
  },
  apis: ['./routes/*.js', './models/*.js', './models/schemas.js', './controllers/*.js']
};

const specs = swaggerJsdoc(options);
module.exports = specs;

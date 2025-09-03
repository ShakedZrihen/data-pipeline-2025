const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
require('dotenv').config();

// Swagger imports
const swaggerUi = require('swagger-ui-express');
const swaggerSpecs = require('./swaggerConfig');

const errorHandler = require('./middleware/errorHandler');
const limiter = require('./middleware/rateLimiter');

const supermarketRoutes = require('./routes/supermarkets');
const productRoutes = require('./routes/products');


const app = express();
const PORT = process.env.PORT || 3001;

app.use(helmet());
app.use(cors());

app.use(limiter);

// Swagger UI route
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpecs, {
  customCss: '.swagger-ui .topbar { display: none }',
  customSiteTitle: 'Supermarket API Documentation',
  swaggerOptions: {
    docExpansion: 'list',
    filter: true,
    showRequestHeaders: true,
    tryItOutEnabled: true
  }
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

app.use('/api/supermarkets', supermarketRoutes);
app.use('/api/products', productRoutes);
// app.use('/api/analytics', analyticsRoutes);  // Commented out - route doesn't exist
// app.use('/api/users', userRoutes);           // Commented out - route doesn't exist

app.get('/', (req, res) => {
  res.json({
    message: 'Supermarket Product API',
    version: '1.0.0',
    endpoints: {
      supermarkets: '/api/supermarkets',
      products: '/api/products',
      // analytics: '/api/analytics',  // Commented out - route doesn't exist
      // users: '/api/users',          // Commented out - route doesn't exist
      health: '/health'
    },
    documentation: 'API documentation available at /api-docs',
    swagger: '/api-docs'
  });
});

app.use('*', (req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: `Route ${req.originalUrl} not found`
  });
});

app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
  console.log(`Supermarkets API: http://localhost:${PORT}/api/supermarkets`);
  console.log(`Products API: http://localhost:${PORT}/api/products`);
});

process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down gracefully');
  process.exit(0);
});

module.exports = app;

const Joi = require("joi");

// Validation schemas
const schemas = {
  supermarketId: Joi.object({
    supermarket_id: Joi.number().integer().positive().required(),
  }),

  barcode: Joi.object({
    barcode: Joi.string().min(1).required(),
  }),

  productFilters: Joi.object({
    name: Joi.string().optional(),
    promo: Joi.boolean().optional(),
    min_price: Joi.number().positive().optional(),
    max_price: Joi.number().positive().optional(),
    supermarket_id: Joi.number().integer().positive().optional(),
  }),

  supermarketProducts: Joi.object({
    supermarket_id: Joi.number().integer().positive().required(),
    search: Joi.string().optional(),
  }),
};

// Validation middleware
const validate = (schemaName) => {
  return (req, res, next) => {
    const schema = schemas[schemaName];
    if (!schema) {
      return res.status(500).json({ error: "Validation schema not found" });
    }

    const { error } = schema.validate(req.params);
    if (error) {
      return res.status(400).json({
        error: "Validation error",
        message: error.details[0].message,
      });
    }

    next();
  };
};

// Query validation middleware
const validateQuery = (schemaName) => {
  return (req, res, next) => {
    const schema = schemas[schemaName];
    if (!schema) {
      return res.status(500).json({ error: "Validation schema not found" });
    }

    const { error } = schema.validate(req.query);
    if (error) {
      return res.status(400).json({
        error: "Validation error",
        message: error.details[0].message,
      });
    }

    next();
  };
};

module.exports = {
  validate,
  validateQuery,
  schemas,
};

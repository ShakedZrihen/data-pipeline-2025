const errorHandler = (err, req, res, next) => {
  console.error('Error:', err);

  let statusCode = 500;
  let message = 'Internal Server Error';

  if (err.name === 'ValidationError') {
    statusCode = 400;
    message = err.message;
  } else if (err.name === 'CastError') {
    statusCode = 400;
    message = 'Invalid ID format';
  } else if (err.code === '23505') { 
    statusCode = 409;
    message = 'Resource already exists';
  } else if (err.code === '23503') { 
    statusCode = 400;
    message = 'Invalid reference';
  } else if (err.message) {
    message = err.message;
  }

  res.status(statusCode).json({
    error: 'Error',
    message: message,
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
  });
};

module.exports = errorHandler;

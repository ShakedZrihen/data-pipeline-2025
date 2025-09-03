FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Environment variables will be passed from docker-compose.yml
# No need to create .env file in container

# Expose port
EXPOSE 3001

# Default command
CMD ["npm", "start"]

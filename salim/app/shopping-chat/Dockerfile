# Node.js base image - use Node 20 for better compatibility
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy application code
COPY . .

# Create .env file with default values for Docker
RUN echo 'NODE_ENV=development' > .env && \
    echo 'PORT=3001' >> .env && \
    echo 'SALIM_API_URL=http://api:8000' >> .env && \
    echo 'ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY:-}' >> .env

# Expose ports
EXPOSE 3001 5173

# Start both the proxy server and Vite dev server (skip build for development)
CMD ["npm", "run", "start"]
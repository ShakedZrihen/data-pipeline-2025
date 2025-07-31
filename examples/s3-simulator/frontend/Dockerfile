FROM node:18-alpine

WORKDIR /app

COPY package.json .
RUN npm install

COPY . .

EXPOSE 3000

# Enable hot reloading by setting environment variables
ENV CHOKIDAR_USEPOLLING=true
ENV WDS_SOCKET_HOST=0.0.0.0
ENV WDS_SOCKET_PORT=3000

CMD ["npm", "start"]
docker run -d --name mongo \
  -p 27017:27017 \
  -v mongo_data:/data/db \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=example \
  mongo:7 
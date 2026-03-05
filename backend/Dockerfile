FROM node:18-alpine

WORKDIR /app

# Copy package files from current directory (backend/)
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy application code
COPY . .

# Generate self-signed TLS certificate for broker-to-broker communication
RUN apk add --no-cache openssl \
  && mkdir -p /app/certs \
  && openssl req -x509 -nodes -newkey rsa:2048 \
       -keyout /app/certs/broker-key.pem \
       -out /app/certs/broker-cert.pem \
       -days 365 \
       -subj "/CN=broker"

# Expose broker port (default)
EXPOSE 5000

# Health check - use https with rejectUnauthorized:false for self-signed cert
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "require('https').get({hostname:'127.0.0.1',port:5000,path:'/health',rejectUnauthorized:false},(r)=>{if(r.statusCode!==200)throw new Error(r.statusCode)})"

# Start broker
CMD ["node", "app.js"]

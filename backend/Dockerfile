FROM node:18-alpine

WORKDIR /app

# Copy package files from current directory (backend/)
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy application code
COPY . .

# Expose broker port (default)
EXPOSE 5000

# Health check - use 127.0.0.1 explicitly
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD node -e "require('http').get('http://127.0.0.1:5000/health', (r) => {if (r.statusCode !== 200) throw new Error(r.statusCode)})"

# Start broker
CMD ["node", "app.js"]

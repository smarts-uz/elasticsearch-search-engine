# Use official Node.js lightweight Alpine image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json first for caching
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy all JS files
COPY . .

# Install additional tools (bash, curl, timezone data)
RUN apk add --no-cache bash curl tzdata \
    && cp /usr/share/zoneinfo/Asia/Tashkent /etc/localtime \
    && echo "Asia/Tashkent" > /etc/timezone

# Expose port if needed (listener doesn't need a port)
# EXPOSE 3000

# Start the container with start.js (initial sync + listener)
CMD ["node", "start.js"]

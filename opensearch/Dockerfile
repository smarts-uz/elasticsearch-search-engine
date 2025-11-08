FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY sync.js scheduler.js ./

# kerakli utilitlar
RUN apk add --no-cache bash curl tzdata

# vaqt zonasini o‘rnatish (optional)
ENV TZ=Asia/Tashkent

# container doimiy ishlaydi
CMD ["node", "scheduler.js"]

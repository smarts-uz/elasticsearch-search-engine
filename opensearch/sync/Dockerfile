FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY scheduler.js utils.js sync.js ./

RUN apk add --no-cache bash curl tzdata
ENV TZ=Asia/Tashkent

CMD ["node", "scheduler.js"]

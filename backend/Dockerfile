FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install --force

COPY . .
RUN npm run build

# ---- Runtime ----
FROM node:20-alpine

RUN apk add --no-cache ffmpeg

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev --force

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules/.bin/sequelize ./node_modules/.bin/sequelize

# Cria diretório para uploads e sessões WhatsApp
RUN mkdir -p /app/public/temp && mkdir -p /app/.wwebjs_auth

EXPOSE 8080

CMD ["node", "dist/server.js"]

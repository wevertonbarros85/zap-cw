FROM node:20-alpine AS builder
RUN apk add --no-cache git python3 make g++
WORKDIR /app
COPY package*.json ./
RUN npm install --force
COPY . .
# Ajusta .sequelizerc para usar dist/ (arquivos compilados)
RUN printf "const path=require('path');\nmodule.exports={'config':path.resolve('config','database.js'),'models-path':path.resolve('dist','models'),'seeders-path':path.resolve('dist','database','seeds'),'migrations-path':path.resolve('dist','database','migrations')};" > .sequelizerc
RUN npm run build || true

FROM node:20-alpine
RUN apk add --no-cache ffmpeg git python3 make g++
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev --force
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/.sequelizerc ./.sequelizerc
COPY --from=builder /app/config ./config
RUN mkdir -p /app/public/temp && mkdir -p /app/.wwebjs_auth
EXPOSE 8080
CMD ["node", "dist/server.js"]

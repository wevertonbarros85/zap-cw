FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install --force --legacy-peer-deps

COPY . .

# Build recebe a URL do backend via ARG (definida no docker-compose)
ARG REACT_APP_BACKEND_URL
ARG REACT_APP_HOURS_CLOSE_TICKETS_AUTO=24
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL
ENV REACT_APP_HOURS_CLOSE_TICKETS_AUTO=$REACT_APP_HOURS_CLOSE_TICKETS_AUTO

RUN NODE_OPTIONS=--openssl-legacy-provider GENERATE_SOURCEMAP=false npm run build

# ---- Servidor nginx ----
FROM nginx:alpine

COPY --from=builder /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

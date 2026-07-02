# Etapa 1: Construção (Build) - Usando Node 14 para compatibilidade
FROM node:14 as build
WORKDIR /app

COPY package*.json ./
# Limpeza preventiva de cache
RUN npm cache clean --force
RUN npm install --legacy-peer-deps

COPY . .

# Argumento para injetar a URL do backend
ARG REACT_APP_BACKEND_URL
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL

RUN npm run build

# Etapa 2: Servidor Web (Nginx)
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN npm run build

FROM node:20-alpine

COPY --from=builder /app/dist /usr/share/dashboard/dist

RUN npm install -g http-server

EXPOSE 3005

CMD ["http-server", "/usr/share/dashboard/dist", "-p", "3005", "-a", "0.0.0.0"]

FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --production
COPY src/ ./src/
RUN apk add --no-cache su-exec
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 9100
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:9100/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "src/index.mjs"]

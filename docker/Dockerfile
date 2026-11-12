FROM oven/bun:1.4.4 AS base
WORKDIR /app

COPY package.json bun.lock* ./
RUN bun install

COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts

ENV NODE_ENV=production
EXPOSE 3000

CMD ["bun", "run", "src/main.ts"]

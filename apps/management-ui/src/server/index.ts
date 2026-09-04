import { buildApp, productionStaticRoot } from "./app.js";
import { readConfig } from "./config.js";

const config = readConfig();
const app = await buildApp(config, config.production ? productionStaticRoot() : undefined);

const shutdown = async () => {
  await app.close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
await app.listen({ host: config.host, port: config.port });

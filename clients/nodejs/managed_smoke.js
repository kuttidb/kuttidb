"use strict";

// Run with: KUTTIDB_SERVER=/path/to/kuttidb node managed_smoke.js DATA_DIR [tcp]
const { Client } = require("./kuttidb_client");
const net = require("net");

function freeLoopbackPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close((error) => error ? reject(error) : resolve(port));
    });
  });
}

async function main() {
  const dataDir = process.argv[2];
  if (!dataDir) throw new Error("managed data directory required");
  const tcp = process.argv[3] === "tcp";
  const port = tcp ? await freeLoopbackPort() : undefined;
  const client = await Client.managed({
    dataDir,
    executable: process.env.KUTTIDB_SERVER,
    idleTimeout: 0.25,
    startupTimeout: 5,
    transport: tcp ? "tcp" : "unix",
    host: tcp ? "127.0.0.1" : undefined,
    port,
  });
  try {
    await client.put("managed-node", Buffer.from("value"));
    const value = await client.get("managed-node");
    if (!value || value.toString() !== "value") throw new Error("managed Node value mismatch");
  } finally {
    await client.close();
  }
}

main().catch((error) => { console.error(error); process.exit(1); });

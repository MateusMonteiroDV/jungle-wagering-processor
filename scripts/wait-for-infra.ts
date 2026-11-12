import { SQSClient, ListQueuesCommand } from "@aws-sdk/client-sqs";
import { loadEnv } from "../src/infrastructure/config/env";

const env = loadEnv();

async function waitForSqs(maxAttempts = 30): Promise<void> {
  const client = new SQSClient({
    region: env.AWS_REGION,
    endpoint: env.SQS_ENDPOINT,
    credentials: {
      accessKeyId: env.AWS_ACCESS_KEY_ID,
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    },
  });

  for (let i = 1; i <= maxAttempts; i++) {
    try {
      await client.send(new ListQueuesCommand({}));
      return;
    } catch {
      await Bun.sleep(1000);
    }
  }
  throw new Error("Timed out waiting for SQS at " + env.SQS_ENDPOINT);
}

async function main() {
  await waitForSqs();
  console.log("Infrastructure (SQS) is reachable.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

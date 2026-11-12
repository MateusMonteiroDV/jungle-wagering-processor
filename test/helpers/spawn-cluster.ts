import { Subprocess } from "bun";
import { waitFor } from "./db";

export interface SpawnedInstance {
  instanceId: string;
  port: number;
  baseUrl: string;
  proc: Subprocess;
  kill(signal?: NodeJS.Signals | number): Promise<number>;
}

let nextPort = 14100;

export function allocatePort(): number {
  nextPort += 1;
  return nextPort;
}

export async function spawnAppInstance(
  envOverrides: Record<string, string>,
  options: { waitForReady?: boolean; readyTimeoutMs?: number } = {},
): Promise<SpawnedInstance> {
  const port = envOverrides.PORT
    ? parseInt(envOverrides.PORT, 10)
    : allocatePort();
  const instanceId = envOverrides.INSTANCE_ID ?? `inst-${port}`;
  const baseUrl = `http://127.0.0.1:${port}`;

  const proc = Bun.spawn(["bun", "src/main.ts"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: String(port),
      INSTANCE_ID: instanceId,
      LOG_LEVEL: "silent",
      ...envOverrides,
    },
    stdout: "ignore",
    stderr: "ignore",
  });

  const instance: SpawnedInstance = {
    instanceId,
    port,
    baseUrl,
    proc,
    async kill(signal: NodeJS.Signals | number = "SIGTERM") {
      try {
        proc.kill(signal);
      } catch {
        // already exited
      }
      if (signal !== "SIGKILL") {
        const res = await Promise.race([
          proc.exited,
          Bun.sleep(2500).then(() => "timeout" as const),
        ]);
        if (res === "timeout") {
          try {
            proc.kill("SIGKILL");
          } catch {}
        }
      }
      return await proc.exited;
    },
  };

  if (options.waitForReady !== false && envOverrides.RUN_HTTP !== "false") {
    await waitFor(
      async () => {
        const res = await fetch(`${baseUrl}/health/ready`);
        return res.status === 200;
      },
      options.readyTimeoutMs ?? 15000,
      100,
    );
  }

  return instance;
}

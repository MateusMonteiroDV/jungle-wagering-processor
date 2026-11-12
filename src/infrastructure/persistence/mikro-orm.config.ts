import { Migrator } from "@mikro-orm/migrations";
import { defineConfig, Options } from "@mikro-orm/postgresql";
import { AppEnv, loadEnv } from "../config/env";
import { Migration20260729000001_InitialSchema } from "./migrations/Migration20260729000001_InitialSchema";
import { ALL_SCHEMAS } from "./schemas";

export function createMikroOrmOptions(envOverride?: Partial<AppEnv>): Options {
  const env = { ...loadEnv(), ...envOverride };
  return defineConfig({
    entities: ALL_SCHEMAS,
    dbName: env.DB_NAME,
    host: env.DB_HOST,
    port: env.DB_PORT,
    user: env.DB_USER,
    password: env.DB_PASSWORD,
    pool: {
      min: 2,
      max: env.DB_POOL_MAX,
    },
    extensions: [Migrator],
    migrations: {
      migrationsList: [
        {
          name: "Migration20260729000001_InitialSchema",
          class: Migration20260729000001_InitialSchema,
        },
      ],
      transactional: true,
      allOrNothing: true,
    },
    allowGlobalContext: false,
    debug: false,
  });
}

export default createMikroOrmOptions();

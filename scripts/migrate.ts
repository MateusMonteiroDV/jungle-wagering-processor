import { MikroORM } from "@mikro-orm/postgresql";
import { createMikroOrmOptions } from "../src/infrastructure/persistence/mikro-orm.config";

async function main() {
  const action = process.argv[2] ?? "up";
  const orm = await MikroORM.init(createMikroOrmOptions());
  try {
    const migrator = orm.getMigrator();
    if (action === "up") {
      await migrator.up();
      console.log("Migrations applied (up).");
    } else if (action === "down") {
      await migrator.down({ to: 0 });
      console.log("Migrations reverted (down).");
    } else if (action === "fresh") {
      await migrator.down({ to: 0 });
      await migrator.up();
      console.log("Migrations refreshed (down -> up).");
    } else {
      throw new Error(`Unknown migrate action '${action}'. Use up | down | fresh`);
    }
  } finally {
    await orm.close(true);
  }
}

main().catch((err) => {
  console.error("Migration script failed:", err);
  process.exit(1);
});

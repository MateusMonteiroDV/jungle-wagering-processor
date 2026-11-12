import { TransientInfrastructureError } from "../domain/errors";

export type FaultPoint =
  | "after_ledger_insert"
  | "crash_after_commit"
  | "crash_after_lease"
  | "transient_sqs_consume";

let runtimeFaultOverride: string | undefined;

export function setFaultInjection(fault: string | undefined): void {
  runtimeFaultOverride = fault;
}

export function getActiveFault(): string {
  return runtimeFaultOverride ?? process.env.FAULT_INJECTION ?? "";
}

export function triggerFaultIfActive(point: FaultPoint): void {
  const active = getActiveFault();
  if (!active) return;

  const activePoints = active.split(",").map((s) => s.trim());
  if (!activePoints.includes(point)) return;

  if (point === "after_ledger_insert") {
    throw new Error("FAULT_INJECTION: simulated failure after ledger insert");
  }

  if (point === "transient_sqs_consume") {
    throw new TransientInfrastructureError(
      "FAULT_INJECTION: simulated transient error during SQS consumption",
    );
  }

  if (point === "crash_after_commit" || point === "crash_after_lease") {
    process.exit(137);
  }
}

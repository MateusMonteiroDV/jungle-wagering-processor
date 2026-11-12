export function encodeLedgerCursor(seq: bigint): string {
  return Buffer.from(JSON.stringify({ seq: seq.toString() }), "utf8").toString(
    "base64url",
  );
}

export function decodeLedgerCursor(cursor: string): bigint {
  try {
    const raw = Buffer.from(cursor, "base64url").toString("utf8");
    const parsed = JSON.parse(raw) as { seq?: string };
    if (!parsed || typeof parsed.seq !== "string" || !/^\d+$/.test(parsed.seq)) {
      throw new Error("Invalid cursor seq");
    }
    return BigInt(parsed.seq);
  } catch {
    throw new Error("Invalid ledger pagination cursor");
  }
}

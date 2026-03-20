import { getRandomValues } from "node:crypto";

declare const __brand: unique symbol;
export type Brand<B> = { [__brand]: B };
export type Branded<T, B> = T & Brand<B>;

export type Serializable =
  | string
  | number
  | boolean
  | null
  | undefined
  | bigint
  | Date
  | RegExp
  | Serializable[]
  | { [key: string]: Serializable }
  | Map<Serializable, Serializable>
  | Set<Serializable>;

/// Program IDs are in the format developer/program_id
/// e.g. arcmaximizer/hello-arc
export type ProgramId = Branded<string, "ProgramId">;
export function isProgramId(value: string): value is ProgramId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

// SHA-256 hash, often used as a unique identifier for blob data
export type Hash = Branded<string, "Hash">;
export function isHash(value: string): value is Hash {
  return /^[0-9a-f]{64}$/
    .test(value);
}

export type UUID = Branded<string, "UUID">;
export function isUUID(value: string): value is UUID {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/
    .test(value);
}
export interface EventContext {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
  exists(key: string): Promise<boolean>;
  call(app: string, input: unknown): Promise<unknown>;
}

export function generateUUIDv7(): string {
  const buf = new Uint8Array(16);

  // timestamp in ms
  const now = BigInt(Date.now());
  buf[0] = Number((now >> 40n) & 0xffn);
  buf[1] = Number((now >> 32n) & 0xffn);
  buf[2] = Number((now >> 24n) & 0xffn);
  buf[3] = Number((now >> 16n) & 0xffn);
  buf[4] = Number((now >> 8n) & 0xffn);
  buf[5] = Number(now & 0xffn);

  // random bytes for the rest
  getRandomValues(buf.subarray(6));

  // set version (7)
  buf[6] = (buf[6]! & 0x0f) | 0x70;
  // set variant (10xxxxxx)
  buf[8] = (buf[8]! & 0x3f) | 0x80;

  const hex = [...buf].map((b) => b.toString(16).padStart(2, "0")).join("");

  return (
    hex.slice(0, 8) + "-" +
    hex.slice(8, 12) + "-" +
    hex.slice(12, 16) + "-" +
    hex.slice(16, 20) + "-" +
    hex.slice(20)
  );
}

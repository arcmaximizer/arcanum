import { Result, ResultAsync } from "neverthrow";

const brand = Symbol("brand");

export type Brand<T, U> = T & {
  [brand]: U;
};
export type RemoveBrand<T> = T[Exclude<keyof T, typeof brand>];

export type ProgramId = Brand<string, "ProgramId">;
export type Hash = Brand<string, "Hash">;

export async function hashBlob(blob: Blob): Promise<Hash> {
  const arrayBuffer = await blob.arrayBuffer();
  const hashBuffer = await crypto.subtle.digest("SHA-256", arrayBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("") as Hash;
}

export function isHash(value: string): value is Hash {
  return /^[A-Fa-f0-9]{64}$/.test(value);
}

export function wrap<T, E>(fn: () => Promise<Result<T, E>>): ResultAsync<T, E> {
  return ResultAsync.fromSafePromise(fn()).andThen((inner) => inner);
}

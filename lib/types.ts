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

// Program IDs are in the format developer/program_id
// e.g. arcmaximizer/hello-arc
export type ProgramId = Branded<string, "ProgramId">;

export function isProgramId(value: string): value is ProgramId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

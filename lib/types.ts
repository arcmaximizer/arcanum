declare const __brand: unique symbol;
export type Brand<B> = { [__brand]: B };
export type Branded<T, B> = T & Brand<B>;

export class TaggedError<Tag extends string> extends Error {
  readonly _tag: Tag;

  constructor(tag: Tag, message?: string) {
    super(message);
    this._tag = tag;
    this.name = tag;
  }
}

export function createError<Tag extends string, Props = any>(tag: Tag) {
  return class extends TaggedError<Tag> {
    constructor(public readonly props: Props, message?: string) {
      super(tag, message);
      Object.assign(this, props);
    }
  } as unknown as new (props: Props) => TaggedError<Tag> & Readonly<Props>;
}

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

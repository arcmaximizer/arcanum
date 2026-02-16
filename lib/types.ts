declare const __brand: unique symbol;
export type Brand<B> = { [__brand]: B };
export type Branded<T, B> = T & Brand<B>;
export type Unbrand<T> = T extends Brand<infer U, any> ? U : T;

export class TaggedError<Tag extends string> extends Error {
  readonly _tag: Tag;

  constructor(tag: Tag, message?: string) {
    super(message);
    this._tag = tag;
    this.name = tag;
  }
}

export function createError<Tag extends string, Props = {}>(tag: Tag) {
  return class extends TaggedError<Tag> {
    constructor(public readonly props: Props, message?: string) {
      super(tag, message);
      Object.assign(this, props);
    }
  } as new (props: Props) => TaggedError<Tag> & Readonly<Props>;
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

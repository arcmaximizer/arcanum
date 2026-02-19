import { Branded, createError, ProgramId, Serializable } from "./types";

export const NetworkError = createError<"NetworkError">("NetworkError");

export type NetworkResult = null | Message | NetworkError;

export interface NetworkService {
  send(message: Message): Promise<NetworkResult>;
  send(
    from: ProgramId,
    to: ReceiverId,
    content: Serializable | string | Uint8Array,
    extraData?: Serializable,
    replyTo?: number,
  ): Promise<NetworkResult>;
}

export interface Message {
  from: ProgramId;
  to: ReceiverId;
  content: Serializable | string | Uint8Array;
  extraData?: Serializable;
  replyTo?: number;
  id?: number;
}

// Receivers are other programs running on an Arcanum
// It is in the form program_id@node_id or alias@node_id
// e.g. arcmaximizer/hello-arc@my-node
// If running on the same node, you can use @local as the node_id
export type ReceiverId = Branded<string, "ReceiverId">;

export function isReceiverId(value: string): value is ReceiverId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)@([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

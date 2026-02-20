import { Branded, ProgramId, Serializable } from "./types.ts";
import { err, ok, Result, ResultAsync } from "neverthrow";

export class NetworkError extends Error {}

export interface NetworkService {
  send(message: Message): ResultAsync<Message | undefined, NetworkError>;
  send(
    from: ProgramId,
    to: ReceiverId,
    content: Serializable | string | Uint8Array,
    extraData?: Serializable,
    replyTo?: number,
  ): ResultAsync<Message | undefined, NetworkError>;
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

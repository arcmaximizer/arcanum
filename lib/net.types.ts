import { Branded, createError, Serializable } from "./types";

export const NetworkError = createError<"NetworkError", {}>("NetworkError");

export type NetworkResult = null | Message | NetworkError;

export interface NetworkService {
  send(message: Message): Promise<NetworkResult>;
  send(
    from: ProgramId,
    to: ReceiverId,
    content: Serializable | string | Uint8Array,
    capabilities?: Capability[],
    extraData?: Serializable,
    replyTo?: number,
  ): Promise<NetworkResult>;
}

export interface Message {
  from: ProgramId;
  to: ReceiverId;
  content: Serializable | string | Uint8Array;
  capabilities?: Capability[];
  extraData?: Serializable;
  replyTo?: number;
  id?: number;
}

// Receivers are other programs running on an Arcanum
// It is in the form program_id@node_id or alias@node_id
// e.g. arcmaximizer/hello-arc@my-node
// If running on the same node, you can use @local as the node_id
export type ReceiverId = Branded<string, "ReceiverId">;

// Program IDs are in the format developer/program_id
// e.g. arcmaximizer/hello-arc
export type ProgramId = Branded<string, "ProgramId">;

// Capability IDs are in the format developer/program_id:capability
// e.g. arcmaximizer/hello-arc:capability
export type CapabilityId = Branded<string, "ProgramId">;

// Capabilities are used for permissioning
// They will be authenticated by the runtime when a request is sent or received
export interface Capability {
  id: CapabilityId;
  metadata: Serializable;
}

export function isProgramId(value: string): value is ProgramId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

export function isReceiverId(value: string): value is ReceiverId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)@([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

export function isCapabilityId(value: string): value is CapabilityId {
  return /^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*):([a-z0-9](?:-?[a-z0-9])*)$/
    .test(value);
}

import { Branded } from "../lib/types";

export interface ArcnetService {
  send(message: Message): Promise<boolean>;
  send(to: ReceiverId, content: string | Uint8Array);
}

export interface Message {
  from: ProgramId;
  to: ReceiverId;
  content: string | Uint8Array;
  extraData: any;
}

// Receivers are other programs running on an Arcanum
// It is in the form program_id@node_id or alias@node_id
// e.g.
//     arcmaximizer/hello-arc@my-node
// or  #hello-arc-alias@my-node
// If running on the same node, you can use @local
export type ReceiverId = Branded<string, "ReceiverId">;

// Program IDs are in the format developer/program_id
// e.g.
//     arcmaximizer/hello-arc
// or  #hello-arc-alias
export type ProgramId = Branded<string, "ProgramId">;

// Capabilities are in the format
// They will be authenticated by the runtime
export type Capability = Branded<string, "Capability">;

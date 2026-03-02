// Immutable store that allows content referencing using unique IDs
// Fetch app code and the data of any given event
// Also handles querying of state at certain checkpoints

import { ProgramId, UUID } from "../lib/types.ts";

export class AppState {
  id: ProgramId;
  constructor(id: ProgramId) {
    this.id = id;
  }
  
  // lazy get - try to get it from the table "head" if node is head
  // 
  get(node: UUID, key: string): string {
    return "hello world"
  }

  set(node: UUID, key: string, value: string) {
    
  }
  
  // set except it ignores whether this node already has the key set
  setDangerously(node: UUID, key: string, value: string) {
    
  }

  checkpoint(node: UUID) {
    
  }
}

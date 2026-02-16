import { Serializable } from "./types";

export interface ExecutionContext {
  setTimer(data: Serializable, scheduledTime: number | Date): Promise<number>;
  send(): Promise<number>;
}

// Runner.ts
import { err, ok, Result } from "neverthrow";

class WorkerExistsError extends Error {}

class Runner {
  workers: Map<string, Worker> = new Map();

  async loadProcess(worker: string, templateId: number, data: Blob) {
  }

  async createWorker(id: string): Promise<Result<void, WorkerExistsError>> {
    if (this.workers.has(id)) return err(new WorkerExistsError());
    this.workers.set(id, new Worker("./worker.ts"));

    return ok();
  }

  async createProcess(worker: number, templateId: number, procId: string) {
  }

  async execute(worker: number, procId: string) {
  }
}

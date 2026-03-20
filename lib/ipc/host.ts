import type { IPCMethodHandler } from "./types.ts";
import type { HostIPC } from "./api.ts";

export function createHostIPC(worker: Worker, options: {
  timeout?: number;
} = {}): HostIPC {
  const handlers = new Map<string, IPCMethodHandler>();
  const pendingRequests = new Map<string, {
    resolve: (value: unknown) => void;
    reject: (reason: unknown) => void;
    timeoutId?: number;
  }>();

  const ipc: HostIPC = {
    worker,
    timeout: options.timeout,

    async call<R = unknown>(method: string, body?: unknown): Promise<R> {
      const localHandler = handlers.get(method);
      if (localHandler) {
        return (await Promise.resolve(localHandler(body))) as R;
      }

      const id = crypto.randomUUID();
      const { timeout } = ipc;

      return new Promise<R>((resolve, reject) => {
        const timeoutId = timeout
          ? setTimeout(() => {
            pendingRequests.delete(id);
            reject(new Error(`IPC call "${method}" timed out after ${timeout}ms`));
          }, timeout) as unknown as number
          : undefined;

        pendingRequests.set(id, {
          resolve: resolve as (value: unknown) => void,
          reject,
          timeoutId,
        });

        worker.postMessage({ id, method, body, type: "req" });
      });
    },

    on(method: string, handler: IPCMethodHandler): void {
      handlers.set(method, handler);
    },

    off(method: string): void {
      handlers.delete(method);
    },

    terminate(): void {
      for (const [, pending] of pendingRequests) {
        if (pending.timeoutId) clearTimeout(pending.timeoutId);
        pending.reject(new Error("IPC terminated"));
      }
      pendingRequests.clear();
      handlers.clear();
      worker.removeEventListener("message", handleMessage);
    },
  };

  function handleMessage(event: MessageEvent) {
    const msg = event.data;

    if (!msg || typeof msg !== "object") return;
    if (!msg.id || typeof msg.id !== "string") return;

    if (msg.type === "req") {
      const handler = handlers.get(msg.method);

      if (handler) {
        Promise.resolve()
          .then(() => handler(msg.body))
          .then((result) => {
            worker.postMessage({ id: msg.id, body: result, type: "res" });
          })
          .catch((error) => {
            worker.postMessage({
              id: msg.id,
              body: error instanceof Error ? error.message : String(error),
              type: "err",
            });
          });
      } else {
        worker.postMessage({
          id: msg.id,
          body: `Unknown method: ${msg.method}`,
          type: "err",
        });
      }
    } else {
      const pending = pendingRequests.get(msg.id);
      if (pending) {
        if (pending.timeoutId) clearTimeout(pending.timeoutId);
        pendingRequests.delete(msg.id);
        if (msg.type === "err") {
          pending.reject(new Error(String(msg.body)));
        } else {
          pending.resolve(msg.body);
        }
      }
    }
  }

  worker.addEventListener("message", handleMessage);

  return ipc;
}

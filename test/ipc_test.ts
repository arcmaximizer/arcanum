import { assertEquals, assertRejects } from "jsr:@std/assert";
import { createHostIPC } from "../lib/ipc/host.ts";

function createWorker(name: string): Worker {
  return new Worker(new URL(`./workers/${name}.ts`, import.meta.url).href, {
    type: "module",
  });
}

function waitForReady(worker: Worker): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout")), 5000);
    worker.onmessage = (e) => {
      if (e.data?.type === "ready") {
        clearTimeout(timer);
        resolve();
      }
    };
    worker.onerror = (e) => {
      clearTimeout(timer);
      reject(e);
    };
  });
}

Deno.test("IPC - Worker to Host RPC", async () => {
  const w = createWorker("simple");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  assertEquals(await ipc.call<string>("greet", "World"), "Hello, World!");
  assertEquals(await ipc.call<number>("add", [2, 3]), 5);
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Host to Worker RPC", async () => {
  const w = createWorker("callback");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  ipc.on("double", (n) => (n as number) * 2);
  assertEquals(await ipc.call<number>("double", 21), 42);
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Error propagation from handler", async () => {
  const w = createWorker("simple");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  await assertRejects(
    () => ipc.call("fail"),
    Error,
    "Intentional failure",
  );
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Unknown method returns error", async () => {
  const w = createWorker("callback");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  await assertRejects(
    () => ipc.call("nonexistent"),
    Error,
    "Unknown method: nonexistent",
  );
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Handler on/off", async () => {
  const w = createWorker("callback");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  ipc.on("method1", () => "first");
  assertEquals(await ipc.call("method1"), "first");
  ipc.off("method1");
  ipc.on("method1", () => "second");
  assertEquals(await ipc.call("method1"), "second");
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Concurrent requests", async () => {
  const w = createWorker("concurrent");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  const [a, b, c] = await Promise.all([
    ipc.call<number>("delay", 50),
    ipc.call<number>("delay", 10),
    ipc.call<number>("delay", 30),
  ]);
  assertEquals(a, 50);
  assertEquals(b, 10);
  assertEquals(c, 30);
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Timeout", async () => {
  const w = createWorker("timeout");
  await waitForReady(w);
  const ipc = createHostIPC(w, { timeout: 100 });
  await assertRejects(
    () => ipc.call("slow"),
    Error,
    "timed out",
  );
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Terminate rejects pending", async () => {
  const w = createWorker("timeout");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  const p = ipc.call("hang");
  ipc.terminate();
  await assertRejects(() => p, Error, "IPC terminated");
});

Deno.test("IPC - Bidirectional RPC", async () => {
  const w = createWorker("bidirectional");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  ipc.on("processInHost", (msg) => `Host processed: ${msg}`);
  assertEquals(
    await ipc.call<string>("callHost", "hello"),
    "Host processed: hello - processed in worker",
  );
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Structured clone types", async () => {
  const w = createWorker("clone");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  const obj = { nested: { value: 42 }, array: [1, 2, 3] };
  const r = (await ipc.call("echo", obj)) as typeof obj;
  assertEquals(r.nested.value, 42);
  assertEquals(r.array, [1, 2, 3]);
  ipc.terminate();
  w.terminate();
});

Deno.test("IPC - Worker IPC self-call", async () => {
  const w = createWorker("selfcall");
  await waitForReady(w);
  const ipc = createHostIPC(w);
  assertEquals(await ipc.call<number>("computeAsync", 5), 11);
  ipc.terminate();
  w.terminate();
});

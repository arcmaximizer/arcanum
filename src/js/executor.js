// Executor ops for JS runtime

export function kvGet(process, key) {
  return Deno.core.ops.op_kv_get(process, key);
}

export function kvSet(process, key, value) {
  Deno.core.ops.op_kv_set(process, key, value);
}

export function lock(process) {
  Deno.core.ops.op_lock(process);
}

export function unlock(process) {
  Deno.core.ops.op_unlock(process);
}

export function send(target, message) {
  return Deno.core.ops.op_send(target, message);
}

export function recordChunk(status) {
  Deno.core.ops.op_record_chunk(status);
}

export function startChunk() {
  Deno.core.ops.op_start_chunk();
}

export function addInput(type, value) {
  Deno.core.ops.op_add_input(type, value);
}
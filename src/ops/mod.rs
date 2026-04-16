deno_core::extension!(
    executor_ops,
    ops = [
        crate::executor::op_kv_get,
        crate::executor::op_kv_set,
        crate::executor::op_lock,
        crate::executor::op_unlock,
        crate::executor::op_send,
        crate::executor::op_record_chunk,
        crate::executor::op_start_chunk,
        crate::executor::op_add_input,
    ]
);

local _yield = coroutine.yield

local function syscall(syscall_type, ...)
    return _yield({type = syscall_type, args = {...}})
end

local kv = {}

function kv.get(key)
    return syscall("kv_get", key)
end
function kv.set(key, value)
    return syscall("kv_set", key, value)
end

local function call(target, ...)
    return syscall("call", target, ...)
end

local function notify(target, ...)
    return syscall("notify", target, ...)
end

rawset(_G, "http", http)
rawset(_G, "kv", kv)
rawset(_G, "call", call)
rawset(_G, "notify", notify)
rawset(_G, "coroutine", nil)
rawset(_G, "syscall", nil)

return function(main_fn)
    return main_fn
end

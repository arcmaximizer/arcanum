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

local http = {}

function http.get(target, ...)
    return call("sys/http", "get", target, ...)
end

function http.post(target, ...)
    return call("sys/http", "post", target, ...)
end

function http.put(target, ...)
    return call("sys/http", "put", target, ...)
end

function http.delete(target, ...)
    return call("sys/http", "delete", target, ...)
end

function http.request(target, verb, ...)
    return call("sys/http", verb, target, ...)
end

local function call(target, ...)
    local result = syscall("call", target, ...)
    if type(result) == "table" and result.error ~= nil then
        error(result.error)
    end
    if type(result) == "table" then
        return result.data
    end
    return result
end

local function notify(target, ...)
    return syscall("notify", target, ...)
end

function spawn(template, name)
    return syscall("spawn", template, name)
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

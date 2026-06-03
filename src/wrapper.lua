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

local sql = {}

function sql.exec(stmt, ...)
    local params = {...}
    local result
    if #params > 0 then
        result = syscall("sql_exec", stmt, params)
    else
        result = syscall("sql_exec", stmt)
    end
    if type(result) == "table" and result.error ~= nil then
        error(result.error)
    end
    return result
end

function sql.query(stmt, ...)
    local params = {...}
    local result
    if #params > 0 then
        result = syscall("sql_query", stmt, params)
    else
        result = syscall("sql_query", stmt)
    end
    if type(result) == "table" and result.error ~= nil then
        error(result.error)
    end
    return result
end

local http = {}

local function http_request(method, url, options)
    options = options or {}
    local req = {
        method = method,
        url = url,
        headers = options.headers,
        query = options.query,
        body = options.body,
        timeoutMs = options.timeoutMs,
    }
    return call("^sys/http", req)
end

function http.get(url, options)
    return http_request("GET", url, options)
end

function http.post(url, options)
    return http_request("POST", url, options)
end

function http.put(url, options)
    return http_request("PUT", url, options)
end

function http.delete(url, options)
    return http_request("DELETE", url, options)
end

function http.request(url, verb, options)
    return http_request(string.upper(verb), url, options)
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

function register(template, name)
    return syscall("register", template, name)
end

rawset(_G, "http", http)
rawset(_G, "kv", kv)
rawset(_G, "sql", sql)
rawset(_G, "call", call)
rawset(_G, "notify", notify)
rawset(_G, "register", register)
rawset(_G, "coroutine", nil)
rawset(_G, "syscall", nil)

return function(main_fn)
    return main_fn
end

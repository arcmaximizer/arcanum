use mlua::Lua;
use serde_json::Value as JsonValue;

pub fn mlua_to_json(value: &mlua::Value) -> JsonValue {
    match value {
        mlua::Value::Nil => JsonValue::Null,
        mlua::Value::Boolean(b) => JsonValue::Bool(*b),
        mlua::Value::Integer(i) => JsonValue::Number(serde_json::Number::from(*i)),
        mlua::Value::Number(n) => {
            let num = serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0));
            JsonValue::Number(num)
        }
        mlua::Value::String(s) => JsonValue::String(s.to_string_lossy()),
        mlua::Value::Table(t) => {
            let mut is_array = true;
            let mut array = Vec::new();
            let mut map = serde_json::Map::new();

            for pair in t.pairs::<mlua::Value, mlua::Value>() {
                if let Ok((k, v)) = pair {
                    match &k {
                        mlua::Value::Integer(idx) if *idx == array.len() as i64 + 1 => {
                            array.push(mlua_to_json(&v));
                        }
                        mlua::Value::String(s) => {
                            is_array = false;
                            map.insert(s.to_string_lossy(), mlua_to_json(&v));
                        }
                        _ => {
                            is_array = false;
                            map.insert(format!("{:?}", k), mlua_to_json(&v));
                        }
                    }
                }
            }

            if is_array && !array.is_empty() {
                JsonValue::Array(array)
            } else if !map.is_empty() {
                JsonValue::Object(map)
            } else if array.is_empty() {
                JsonValue::Object(serde_json::Map::new())
            } else {
                JsonValue::Array(array)
            }
        }
        _ => JsonValue::Null,
    }
}

pub fn json_to_mlua(lua: &Lua, value: &JsonValue) -> mlua::Result<mlua::Value> {
    match value {
        JsonValue::Null => Ok(mlua::Value::Nil),
        JsonValue::Bool(b) => Ok(mlua::Value::Boolean(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(mlua::Value::Integer(i))
            } else {
                Ok(mlua::Value::Number(n.as_f64().unwrap_or(0.0)))
            }
        }
        JsonValue::String(s) => Ok(mlua::Value::String(lua.create_string(s)?)),
        JsonValue::Array(arr) => {
            let table = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                table.set(i + 1, json_to_mlua(lua, v)?)?;
            }
            Ok(mlua::Value::Table(table))
        }
        JsonValue::Object(map) => {
            let table = lua.create_table()?;
            for (k, v) in map {
                let key: mlua::Value = mlua::Value::String(lua.create_string(k)?);
                table.set(key, json_to_mlua(lua, v)?)?;
            }
            Ok(mlua::Value::Table(table))
        }
    }
}

pub fn bytes_to_json_pretty(bytes: &[u8]) -> String {
    let value: JsonValue = rmp_serde::from_slice(bytes).unwrap_or(JsonValue::Null);
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| format!("{bytes:?}"))
}

pub fn mlua_value_to_bytes(value: &mlua::Value) -> Vec<u8> {
    let json = mlua_to_json(value);
    rmp_serde::to_vec(&json).unwrap_or_default()
}

pub fn bytes_to_mlua_value(lua: &Lua, bytes: &[u8]) -> mlua::Value {
    if bytes.is_empty() {
        return mlua::Value::Nil;
    }
    let json: JsonValue = rmp_serde::from_slice(bytes).unwrap_or(JsonValue::Null);
    json_to_mlua(lua, &json).unwrap_or(mlua::Value::Nil)
}

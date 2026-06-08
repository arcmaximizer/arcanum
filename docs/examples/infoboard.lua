local processes = {
  infoboard = {
    id = "infoboard",
    handler = function(ctx, msg)
      local kv = ctx.kv
      local ready = kv.get("ready")

      if msg.type == "get" then
        if ready then
          return {
            name = kv.get("name"),
            bio = kv.get("bio"),
            links = kv.get("links"),
          }
        else
          return { name = "N/A", bio = "Not set up yet." }
        end
      elseif msg.type == "setBio" then
        kv.set("bio", msg.bio)
      elseif msg.type == "setName" then
        kv.set("name", msg.name)
      elseif msg.type == "setLinks" then
        kv.set("links", msg.links)
      end
    end,
  },
}

return processes
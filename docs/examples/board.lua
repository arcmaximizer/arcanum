local processes = {
  board = {
    id = "board",
    handler = function(ctx, msg)
      local msg_type = msg.type

      if msg_type == "addPost" then
        return addPost(ctx, msg.content)
      elseif msg_type == "addComment" then
        return addComment(ctx, msg.target, msg.content)
      elseif msg_type == "getPosts" then
        return getPosts(ctx, msg.count, msg.cursor)
      elseif msg_type == "getPost" then
        return getPost(ctx, msg.target)
      end
    end,
  },
}

local function addPost(ctx, content)
  return ctx.lists.posts.append({
    from = ctx.id,
    content = content,
    time = os.time(),
  })
end

local function addComment(ctx, target, content)
  return ctx.lists.comments.append({
    target = target,
    from = ctx.id,
    content = content,
    time = os.time(),
  })
end

local function getPosts(ctx, count, cursor)
  if count > 100 then
    error("Max count: 100")
  end

  return ctx.lists.posts.find({
    condition = "id < " .. (cursor or ""),
    maxResults = count,
    sort = "desc",
    extras = { appendId = true },
  })
end

local function getPost(ctx, target)
  local post = ctx.lists.posts.get(target)
  local comments = ctx.lists.comments.find({
    condition = "target = " .. target,
    extras = { appendId = true },
  })
  return { post = post, comments = comments }
end

return processes
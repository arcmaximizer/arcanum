-- board.lua — example app using sql.exec / sql.query with parameterized queries
-- Runs inside an Arcanum process with its own private SQLite database.
--
-- Usage from another process:
--   call("^arc/forum/board", { type = "addPost",    content = "..." })
--   call("^arc/forum/board", { type = "addComment", target = 1, content = "..." })
--   call("^arc/forum/board", { type = "getPosts",   count = 10, cursor = nil })
--   call("^arc/forum/board", { type = "getPost",    target = 1 })

local processes = {
  board = {
    id = "board",
    handler = function(ctx, msg)
      sql.exec([[
        CREATE TABLE IF NOT EXISTS posts (
          id       INTEGER PRIMARY KEY AUTOINCREMENT,
          author   TEXT    NOT NULL,
          content  TEXT    NOT NULL,
          time     INTEGER NOT NULL
        )
      ]])
      sql.exec([[
        CREATE TABLE IF NOT EXISTS comments (
          id        INTEGER PRIMARY KEY AUTOINCREMENT,
          post_id   INTEGER NOT NULL,
          author    TEXT    NOT NULL,
          content   TEXT    NOT NULL,
          time      INTEGER NOT NULL
        )
      ]])

      local msg_type = msg.type
      if msg_type == "addPost" then
        return addPost(ctx, msg.content)
      elseif msg_type == "addComment" then
        return addComment(ctx, msg.target, msg.content)
      elseif msg_type == "getPosts" then
        return getPosts(ctx, msg.count, msg.cursor)
      elseif msg_type == "getPost" then
        return getPost(ctx, msg.target)
      else
        error("unknown message type: " .. msg_type)
      end
    end,
  },
}

local function addPost(ctx, content)
  sql.exec(
    "INSERT INTO posts (author, content, time) VALUES (?, ?, ?)",
    ctx.id, content, os.time()
  )
  return sql.query("SELECT * FROM posts ORDER BY id DESC LIMIT 1")
end

local function addComment(ctx, postId, content)
  sql.exec(
    "INSERT INTO comments (post_id, author, content, time) VALUES (?, ?, ?, ?)",
    postId, ctx.id, content, os.time()
  )
  return sql.query("SELECT * FROM comments ORDER BY id DESC LIMIT 1")
end

local function getPosts(ctx, count, cursor)
  if count == nil then count = 10 end
  if count > 100 then
    error("Max count: 100")
  end

  if cursor then
    return sql.query(
      "SELECT * FROM posts WHERE id < ? ORDER BY id DESC LIMIT ?",
      cursor, count
    )
  else
    return sql.query(
      "SELECT * FROM posts ORDER BY id DESC LIMIT ?",
      count
    )
  end
end

local function getPost(ctx, target)
  local post = sql.query("SELECT * FROM posts WHERE id = ?", target)
  local comments = sql.query(
    "SELECT * FROM comments WHERE post_id = ?", target
  )
  return { post = post, comments = comments }
end

return processes

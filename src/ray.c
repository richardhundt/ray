#include "ray.h"

static ray_fiber_t* RAY_MAIN;
static uv_async_t   RAY_PUMP;

void ray_pump(void) {
  uv_async_send(&RAY_PUMP);
}

ray_fiber_t* ray_current(lua_State* L) {
  (void)L;
  return (ray_fiber_t*)uv_default_loop()->data;
}

int ray_suspend(ray_fiber_t* fiber) {
  uv_idle_stop(&fiber->hook);
  fiber->flags &= ~RAY_RUNNING;
  return lua_yield(fiber->L, lua_gettop(fiber->L));
}

void ray_sched_cb(uv_idle_t* idle, int status) {
  ray_fiber_t* self = container_of(idle, ray_fiber_t, hook);
  ray_resume(self, LUA_MULTRET);
}

int ray_ready(ray_fiber_t* fiber) {
  uv_idle_start(&fiber->hook, ray_sched_cb);
  return 1;
}

int ray_finish(ray_fiber_t* self) {
  TRACE("finish: %p\n", self);
  while (!ngx_queue_empty(&self->fiber_queue)) {
    int i, n;
    ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
    ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

    ngx_queue_remove(q);
    ngx_queue_init(q);

    n = lua_gettop(self->L);
    for (i = 1; i <= n; i++) {
      lua_pushvalue(self->L, i);
    } 

    lua_xmove(self->L, f->L, n);
    ray_resume(f, n);
  }

  uv_idle_stop(&self->hook);

  if (!ngx_queue_empty(&self->queue)) {
    ngx_queue_remove(&self->queue);
    ngx_queue_init(&self->queue);
  }

  lua_settop(self->L, 0);
  luaL_unref(self->L, LUA_REGISTRYINDEX, self->L_ref);
  luaL_unref(self->L, LUA_REGISTRYINDEX, self->ref);

  return 1;
}

int ray_resume(ray_fiber_t* fiber, int narg) {
  TRACE("resuming: %p\n", fiber);
  if (fiber->flags & RAY_STARTED && lua_status(fiber->L) != LUA_YIELD) {
    TRACE("refusing to resume dead coroutine\n");
    return -1;
  }
  if (fiber == RAY_MAIN) {
    TRACE("refusing to resume RAY_MAIN\n");
    return -1;
  }

  if (narg == LUA_MULTRET) {
    narg = lua_gettop(fiber->L);
  }
  if (!(fiber->flags & RAY_STARTED)) {
    fiber->flags |= RAY_STARTED;
    --narg;
  }
  uv_loop_t* loop = uv_default_loop();
  void* prev = loop->data;
  loop->data = fiber;
  assert(fiber != RAY_MAIN);
  fiber->flags |= RAY_RUNNING;
  int rc = lua_resume(fiber->L, narg);
  loop->data = prev;

  switch (rc) {
    case LUA_YIELD: {
      if (fiber->flags & RAY_RUNNING) {
        ray_ready(fiber);
      }
      break;
    }
    case 0: {
      ray_finish(fiber);
      break;
    }
    default: {
      return luaL_error(RAY_MAIN->L, lua_tostring(fiber->L, -1));
    }
  }
  return 1;
}

int ray_push_error(lua_State* L, uv_errno_t err) {
  lua_settop(L, 0);
  lua_pushnil(L);
  lua_pushstring(L, uv_err_name(err));
  return 2;
}

int ray_run(lua_State* L) {
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);
  return 0;
}

void ray_buf_need(ray_buf_t* buf, size_t len) {
  size_t size = buf->size;
  size_t need = buf->offs + len;
  if (need > buf->size) {
    while (size < need) size *= 2;
    buf->base = (char*)realloc(buf->base, size);
    buf->size = size;
  }
}
void ray_buf_init(ray_buf_t* buf) {
  buf->base = calloc(1, RAY_BUF_SIZE);
  buf->size = RAY_BUF_SIZE;
  buf->offs = 0;
}
void ray_buf_write(ray_buf_t* buf, char* data, size_t len) {
  ray_buf_need(buf, len);
  memcpy(buf->base + buf->offs, data, len);
  buf->offs += len;
}
const char* ray_buf_read(ray_buf_t* buf, size_t* len) {
  *len = buf->offs;
  buf->offs = 0;
  return buf->base;
}
void ray_buf_clear(ray_buf_t* buf) {
  memset(buf->base, 0, buf->size);
  buf->offs = 0;
}

int ray_self(lua_State* L) {
  ray_fiber_t* self = (ray_fiber_t*)(uv_default_loop()->data);
  if (self) {
    lua_rawgeti(L, LUA_REGISTRYINDEX, self->ref);
  }
  else {
    lua_pushnil(L);
  }
  return 1;
}

int ray_fiber_new(lua_State* L) {
  ray_fiber_t* self = lua_newuserdata(L, sizeof(ray_fiber_t));
  luaL_getmetatable(L, "ray.fiber");
  lua_setmetatable(L, -2);
  lua_insert(L, 1);

  lua_pushvalue(L, 1);
  self->ref = luaL_ref(L, LUA_REGISTRYINDEX);

  self->L     = lua_newthread(L);
  self->L_ref = luaL_ref(L, LUA_REGISTRYINDEX);

  self->flags = 0;

  ngx_queue_init(&self->queue);
  ngx_queue_init(&self->fiber_queue);

  uv_idle_init(uv_default_loop(), &self->hook);

  lua_xmove(L, self->L, lua_gettop(L) - 1);
  ray_ready(self);

  return 1;
}

int ray_fiber_ready(lua_State* L) {
  ray_fiber_t* self = (ray_fiber_t*)lua_touserdata(L, 1);
  ray_ready(self);
  return 1;
}

int ray_fiber_suspend(lua_State* L) {
  ray_fiber_t* self = (ray_fiber_t*)lua_touserdata(L, 1);
  return ray_suspend(self);
}

int ray_fiber_join(lua_State* L) {
  ray_fiber_t* self = (ray_fiber_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);
  assert(ngx_queue_empty(&curr->queue));
  ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
  ray_ready(self);
  return ray_suspend(curr);
}

ray_agent_t* ray_agent_new(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_newuserdata(L, sizeof(ray_agent_t));

  self->flags = 0;
  self->count = 0;
  self->data  = NULL;

  ray_buf_init(&self->buf);
  ngx_queue_init(&self->fiber_queue);

  lua_pushvalue(L, 1);
  self->ref = luaL_ref(L, LUA_REGISTRYINDEX);

  return self;
}

/* ========================================================================== */
/* timer                                                                      */
/* ========================================================================== */
int ray_timer_new(lua_State* L) {
  ray_agent_t* self = ray_agent_new(L);
  luaL_getmetatable(L, "ray.timer");
  lua_setmetatable(L, -2);

  int rc = uv_timer_init(uv_default_loop(), &self->h.timer);
  if (rc) return ray_push_error(L, rc);

  return 9;
}

void ray_timer_cb(uv_timer_t* timer, float status) {
  ray_agent_t* self = container_of(timer, ray_agent_t, h);
  ngx_queue_t* tail = ngx_queue_last(&self->fiber_queue);

  while (!ngx_queue_empty(&self->fiber_queue)) {
    ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
    ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

    ngx_queue_remove(q);
    ngx_queue_init(q);

    lua_settop(f->L, 0);
    lua_pushboolean(f->L, 1);
    ray_resume(f, 1);

    if (q == tail) break;
  }
}

int ray_timer_start(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.timer");
  int64_t timeout = luaL_optlong(L, 2, 0L);
  int64_t repeat  = luaL_optlong(L, 3, 0L);

  int rc = uv_timer_start(&self->h.timer, ray_timer_cb, timeout, repeat);
  if (rc) return ray_push_error(L, rc);

  return 1;
}

int ray_timer_stop(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.timer");

  int rc = uv_timer_stop(&self->h.timer);
  if (rc) return ray_push_error(L, rc);

  return 1;
}

int ray_timer_wait(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.timer");
  ray_fiber_t* curr = ray_current(L);
  assert(ngx_queue_empty(&curr->queue));
  ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
  return ray_suspend(curr);
}

/* ========================================================================== */
/* stream                                                                     */
/* ========================================================================== */
void ray_close_cb(uv_handle_t* handle) {
  ray_agent_t* self = container_of(handle, ray_agent_t, h);
  TRACE("closed: %p\n", self);
  ray_fiber_t* curr = (ray_fiber_t*)self->data;
  if (curr) ray_resume(curr, 0);
  if (self->ref != LUA_NOREF) {
    TRACE("UNREF: %i!", self->ref);
    luaL_unref(RAY_MAIN->L, LUA_REGISTRYINDEX, self->ref);
    self->ref = =LUA_NOREF;
  }
}

int ray_close(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  if (!uv_is_closing(&self->h.handle)) {
    ray_fiber_t* curr = ray_current(L);
    self->data = curr;
    uv_close(&self->h.handle, ray_close_cb);
    return ray_suspend(curr);
  }
  return 9;
}

void ray_alloc_cb(uv_handle_t* h, size_t len, uv_buf_t* buf) {
  ray_agent_t* self = container_of(h, ray_agent_t, h);
  if (len > RAY_BUF_SIZE) len = RAY_BUF_SIZE;
  ray_buf_need(&self->buf, len);
  *buf = uv_buf_init(self->buf.base + self->buf.offs, len);
}

void ray_close_null_cb(uv_handle_t* handle) {
  TRACE("running null close cb - data: %p\n", handle->data);
}

void ray_read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  ray_agent_t* self = container_of(stream, ray_agent_t, h);

  uv_read_stop(stream);
  self->flags &= ~RAY_STARTED;

  TRACE("nread: %i\n", (int)nread);
  if (nread >= 0) {
    assert(!ngx_queue_empty(&self->fiber_queue));
    ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
    ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

    ngx_queue_remove(q);
    ngx_queue_init(q);

    lua_pushlstring(f->L, (const char*)buf->base, nread);
    ray_buf_clear(&self->buf);
    ray_resume(f, 8);
  }
  else {
    while (!ngx_queue_empty(&self->fiber_queue)) {
      ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
      ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

      ngx_queue_remove(q);
      ngx_queue_init(q);

      ray_resume(f, ray_push_error(f->L, nread));
    }
    if (!uv_is_closing((uv_handle_t*)stream)) {
      uv_close((uv_handle_t*)stream, ray_close_null_cb);
    }
  }
}

int ray_read(lua_State* L) {
  TRACE("reading\n");
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);

  assert((self->flags & RAY_STARTED) == 0);
  self->flags |= RAY_STARTED;
  uv_read_start(&self->h.stream, ray_alloc_cb, ray_read_cb);

  assert(ngx_queue_empty(&curr->queue));
  ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
  return ray_suspend(curr);
}

void ray_write_cb(uv_write_t* req, int status) {
  ray_fiber_t* curr = container_of(req, ray_fiber_t, r);
  TRACE("finished writing - curr: %p\n", curr);
  ray_resume(curr, 6);
}

int ray_write(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);

  size_t len;
  const char* base = lua_tolstring(L, 2, &len);
  assert((self->flags & RAY_STARTED) == 0);
  uv_buf_t buf = uv_buf_init((char*)base, len);
  TRACE("writing - curr: %p\n", curr);
  uv_write(&curr->r.write, &self->h.stream, &buf, 1, ray_write_cb);

  lua_settop(L, 1);
  return ray_suspend(curr);
}

void ray_shutdown_cb(uv_shutdown_t* req, int status) {
  ray_fiber_t* curr = container_of(req, ray_fiber_t, r);
  TRACE("shutdown: %p\n", curr);
  lua_settop(curr->L, 0);
  lua_pushboolean(curr->L, 1);
  ray_resume(curr, 1);
}

int ray_shutdown(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);

  TRACE("shutdown: self: %p, curr: %p\n", self, curr);
  uv_shutdown_t* req = &curr->r.shutdown;

  int rc = uv_shutdown(req, &self->h.stream, ray_shutdown_cb);
  if (rc) return ray_push_error(L, rc);

  if (self->flags & RAY_STARTED) {
    self->flags &= ~RAY_STARTED;
    uv_read_stop(&self->h.stream);
  }

  return ray_suspend(curr);
}

void ray_listen_cb(uv_stream_t* server, int status) {
  ray_agent_t* self = container_of(server, ray_agent_t, h);
  TRACE("agent: %p\n", self);
  if (!ngx_queue_empty(&self->fiber_queue)) {
    ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
    ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

    ngx_queue_remove(q);
    ngx_queue_init(q);

    ray_agent_t* conn = (ray_agent_t*)lua_touserdata(f->L, 2);
    TRACE("new connection: %p\n", conn);

    int rc = uv_accept(server, &conn->h.stream);
    if (rc) {
      ray_resume(f, ray_push_error(f->L, rc));
      return;
    }
    ray_resume(f, 1);
  }
  else {
    TRACE("count++\n");
    self->count++;
  }
}

int ray_listen(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  int backlog = luaL_optint(L, 2, 128);
  self->count = 0;

  int rc = uv_listen(&self->h.stream, backlog, ray_listen_cb);
  if (rc) return ray_push_error(L, rc);

  return 1;
}

int ray_accept(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_agent_t* conn = (ray_agent_t*)lua_touserdata(L, 2);
  TRACE("accepting - count: %i\n", self->count);
  if (self->count > 0) {
    self->count--;

    int rc = uv_accept(&self->h.stream, &conn->h.stream);
    if (rc) return ray_push_error(L, rc);

    return 1;
  }
  else {
    ray_fiber_t* curr = ray_current(L);
    assert(curr != RAY_MAIN);
    assert(ngx_queue_empty(&curr->queue));
    ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
    return ray_suspend(curr);
  }
}

void ray_connect_cb(uv_connect_t* req, int status) {
  ray_fiber_t* curr = container_of(req, ray_fiber_t, r);
  if (status) {
    ray_resume(curr, ray_push_error(curr->L, status));
  }
  else {
    lua_pushboolean(curr->L, 1);
    ray_resume(curr, 1);
  }
}

/* ========================================================================== */
/* pipe                                                                       */
/* ========================================================================== */
int ray_pipe_new(lua_State* L) {
  int ipc = luaL_optint(L, 1, 0);
  ray_agent_t* self = ray_agent_new(L);
  luaL_getmetatable(L, "ray.pipe");
  lua_setmetatable(L, -2);
  uv_pipe_init(uv_default_loop(), &self->h.pipe, ipc);
  return 1;
}
int ray_pipe_accept(lua_State* L) {
  lua_settop(L, 1);
  ray_pipe_new(L);
  return ray_accept(L);
}
int ray_pipe_bind(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  const char* name = luaL_checkstring(L, 2);

  int rc = uv_pipe_bind(&self->h.pipe, name);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}
int ray_pipe_open(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  uv_file fh = luaL_checkint(L, 2);

  int rc = uv_pipe_open(&self->h.pipe, fh);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}
int ray_pipe_connect(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);
  const char* name = luaL_checkstring(L, 2);

  uv_pipe_connect(&curr->r.connect, &self->h.pipe, name, ray_connect_cb);

  assert(ngx_queue_empty(&curr->queue));
  ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
  return ray_suspend(curr);
}

/* ========================================================================== */
/* TCP                                                                        */
/* ========================================================================== */
int ray_tcp_new(lua_State* L) {
  ray_agent_t* self = ray_agent_new(L);
  luaL_getmetatable(L, "ray.tcp");
  lua_setmetatable(L, -2);
  uv_tcp_init(uv_default_loop(), &self->h.tcp);
  return 1;
}

int ray_tcp_accept(lua_State* L) {
  lua_settop(L, 1);
  ray_tcp_new(L);
  return ray_accept(L);
}

int ray_tcp_connect(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  ray_fiber_t* curr = ray_current(L);

  struct sockaddr_in addr;
  const char* host;
  int port;

  host = luaL_checkstring(L, 2);
  port = luaL_checkint(L, 3);
  uv_ip4_addr(host, port, &addr);

  int rc = uv_tcp_connect(&curr->r.connect, &self->h.tcp, (struct sockaddr *)&addr, ray_connect_cb);
  if (rc) return ray_push_error(L, rc);

  return ray_suspend(curr);
}

int ray_tcp_bind(lua_State* L) {
  ray_agent_t *self = (ray_agent_t*)lua_touserdata(L, 1);

  struct sockaddr_in addr;
  const char* host;
  int port;

  host = luaL_checkstring(L, 2);
  port = luaL_checkint(L, 3);
  uv_ip4_addr(host, port, &addr);

  int rc = uv_tcp_bind(&self->h.tcp, (struct sockaddr*)&addr);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}
int ray_tcp_nodelay(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.tcp");
  luaL_checktype(L, 2, LUA_TBOOLEAN);
  int enable = lua_toboolean(L, 2);
  lua_settop(L, 2);

  int rc = uv_tcp_nodelay(&self->h.tcp, enable);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}
int ray_tcp_keepalive(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.tcp");
  luaL_checktype(L, 2, LUA_TBOOLEAN);
  int enable = lua_toboolean(L, 2);
  unsigned int delay = 0;
  if (enable) delay = luaL_checkint(L, 3);

  int rc = uv_tcp_keepalive(&self->h.tcp, enable, delay);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}
int ray_tcp_getsockname(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.tcp");

  int port = 0;
  char ip[INET6_ADDRSTRLEN];
  int family;

  struct sockaddr_storage addr;
  int len = sizeof(addr);

  int rc = uv_tcp_getsockname(&self->h.tcp, (struct sockaddr*)&addr, &len);
  if (rc) return ray_push_error(L, rc);

  family = addr.ss_family;
  if (family == AF_INET) {
    struct sockaddr_in* addrin = (struct sockaddr_in*)&addr;
    uv_inet_ntop(AF_INET, &(addrin->sin_addr), ip, INET6_ADDRSTRLEN);
    port = ntohs(addrin->sin_port);
  }
  else if (family == AF_INET6) {
    struct sockaddr_in6* addrin6 = (struct sockaddr_in6*)&addr;
    uv_inet_ntop(AF_INET6, &(addrin6->sin6_addr), ip, INET6_ADDRSTRLEN);
    port = ntohs(addrin6->sin6_port);
  }

  lua_newtable(L);
  lua_pushnumber(L, port);
  lua_setfield(L, -2, "port");
  lua_pushnumber(L, family);
  lua_setfield(L, -2, "family");
  lua_pushstring(L, ip);
  lua_setfield(L, -2, "address");

  return 1;
}

int ray_tcp_getpeername(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.tcp");

  int port = 0;
  char ip[INET6_ADDRSTRLEN];
  int family;

  struct sockaddr_storage addr;
  int len = sizeof(addr);

  int rc = uv_tcp_getpeername(&self->h.tcp, (struct sockaddr*)&addr, &len);
  if (rc) return ray_push_error(L, rc);

  family = addr.ss_family;
  if (family == AF_INET) {
    struct sockaddr_in* addrin = (struct sockaddr_in*)&addr;
    uv_inet_ntop(AF_INET, &(addrin->sin_addr), ip, INET6_ADDRSTRLEN);
    port = ntohs(addrin->sin_port);
  }
  else if (family == AF_INET6) {
    struct sockaddr_in6* addrin6 = (struct sockaddr_in6*)&addr;
    uv_inet_ntop(AF_INET6, &(addrin6->sin6_addr), ip, INET6_ADDRSTRLEN);
    port = ntohs(addrin6->sin6_port);
  }

  lua_newtable(L);
  lua_pushnumber(L, port);
  lua_setfield(L, -2, "port");
  lua_pushnumber(L, family);
  lua_setfield(L, -2, "family");
  lua_pushstring(L, ip);
  lua_setfield(L, -2, "address");

  return 1;
}

/* ========================================================================== */
/* UDP                                                                        */
/* ========================================================================== */
int ray_udp_new(lua_State* L) {
  ray_agent_t* self = ray_agent_new(L);
  luaL_getmetatable(L, "ray.udp");
  lua_setmetatable(L, -2);
  uv_udp_init(uv_default_loop(), &self->h.udp);
  return 1;
}
int ray_udp_bind(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.udp");
  const char*  host = luaL_checkstring(L, 2);
  int          port = luaL_checkint(L, 3);

  int flags = 0;
  struct sockaddr_in addr;
  uv_ip4_addr(host, port, &addr);

  int rc = uv_udp_bind(&self->h.udp, (struct sockaddr*)&addr, flags);
  if (rc) return ray_push_error(L, rc);

  lua_settop(L, 1);
  return 1;
}

void ray_udp_send_cb(uv_udp_send_t* req, int status) {
  ray_fiber_t* curr = container_of(req, ray_fiber_t, r);
  if (status < 0) {
    ray_resume(curr, ray_push_error(curr->L, status));
  }
  else {
    ray_resume(curr, 0);
  }
}
int ray_udp_send(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.udp");
  ray_fiber_t* curr = ray_current(L);

  size_t len;

  const char* host = luaL_checkstring(L, 2);
  int         port = luaL_checkint(L, 3);
  const char* mesg = luaL_checklstring(L, 4, &len);

  uv_buf_t buf = uv_buf_init((char*)mesg, len);
  struct sockaddr_in addr;
  uv_ip4_addr(host, port, &addr);

  int rc = uv_udp_send(&curr->r.udp_send, &self->h.udp, &buf, 1, (struct sockaddr*)&addr, ray_udp_send_cb);
  if (rc) return ray_push_error(L, rc);

  return ray_suspend(curr);
}

void ray_udp_recv_cb(uv_udp_t* handle, ssize_t nread, const uv_buf_t *buf,
                     const struct sockaddr* peer, unsigned flags) {
  ray_agent_t* self = container_of(handle, ray_agent_t, h);
  if (nread == 0) return;

  char host[INET6_ADDRSTRLEN];
  int  port = 0;

  if (nread < 0) {
    if (self->flags & RAY_STARTED) {
      self->flags &= ~RAY_STARTED;
      uv_udp_recv_stop(&self->h.udp);
    }

    while (!ngx_queue_empty(&self->fiber_queue)) {
      ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
      ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

      ngx_queue_remove(q);
      ngx_queue_init(q);

      ray_resume(f, ray_push_error(f->L, nread));
    }
    return;
  }

  if (peer->sa_family == PF_INET) {
    struct sockaddr_in* addr = (struct sockaddr_in*)peer;
    uv_ip4_name(addr, host, INET6_ADDRSTRLEN);
    port = addr->sin_port;
  }
  else if (peer->sa_family == PF_INET6) {
    struct sockaddr_in6* addr = (struct sockaddr_in6*)peer;
    uv_ip6_name(addr, host, INET6_ADDRSTRLEN);
    port = addr->sin6_port;
  }

  if (!ngx_queue_empty(&self->fiber_queue)) {
    ngx_queue_t* q = ngx_queue_head(&self->fiber_queue);
    ray_fiber_t* f = ngx_queue_data(q, ray_fiber_t, queue);

    ngx_queue_remove(q);
    ngx_queue_init(q);

    lua_checkstack(f->L, 3);
    lua_pushlstring(f->L, (char*)buf->base, nread);

    lua_pushstring(f->L, host);
    lua_pushinteger(f->L, port);

    ray_buf_clear(&self->buf);
    ray_resume(f, 3);
  }
  else {
    memcpy(self->buf.base, (char*)(void*)&nread, sizeof(nread));
    ray_buf_write(&self->buf, host, strlen(host) + 1);
    ray_buf_write(&self->buf, (char*)(void*)&port, sizeof(port));
    self->count++;
  }
}

void ray_udp_alloc_cb(uv_handle_t* handle, size_t len, uv_buf_t* buf) {
  ray_agent_t* self = container_of(handle, ray_agent_t, h);
  ray_buf_need(&self->buf, sizeof(ssize_t) + len);
  self->buf.offs += sizeof(ssize_t);
  *buf = uv_buf_init(self->buf.base + sizeof(ssize_t), len);
}

int ray_udp_recv(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.udp");
  ray_fiber_t* curr = ray_current(L);

  if (!(self->flags & RAY_STARTED)) {
    int rc = uv_udp_recv_start(&self->h.udp, ray_udp_alloc_cb, ray_udp_recv_cb);
    if (rc) return ray_push_error(L, rc);
    self->flags |= RAY_STARTED;
  }

  if (self->count > 0) {
    ssize_t slen;
    int     port;
    size_t  ulen;
    const char* mesg = ray_buf_read(&self->buf, &ulen);

    memcpy(&slen, mesg, sizeof(ssize_t));
    mesg += sizeof(ssize_t);

    lua_pushlstring(L, mesg, slen);
    mesg += slen;

    lua_pushstring(L, mesg);
    mesg += strlen(mesg) + 1;

    memcpy(&port, mesg, sizeof(int));
    lua_pushinteger(L, port);

    ray_buf_clear(&self->buf);

    self->count--;
    return 3;
  }

  assert(ngx_queue_empty(&curr->queue));
  ngx_queue_insert_tail(&self->fiber_queue, &curr->queue);
  return ray_suspend(curr);
}

static const char* RAY_UDP_OPTS[] = { "join", "leave", NULL };

int ray_udp_membership(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.udp");
  const char*  iaddr = luaL_checkstring(L, 3);
  const char*  maddr = luaL_checkstring(L, 2);

  int option = luaL_checkoption(L, 4, NULL, RAY_UDP_OPTS);
  uv_membership membership = option ? UV_LEAVE_GROUP : UV_JOIN_GROUP;

  int rc = uv_udp_set_membership(&self->h.udp, maddr, iaddr, membership);
  if (rc) return ray_push_error(L, rc);

  return 1;
}

/* ========================================================================== */
/* file system                                                                */
/* ========================================================================== */

/* shamelessly stolen from luvit - bugs are my own */
static int string_to_flags(lua_State* L, const char* str) {
  if (strcmp(str, "r") == 0)
    return O_RDONLY;
  if (strcmp(str, "r+") == 0)
    return O_RDWR;
  if (strcmp(str, "w") == 0)
    return O_CREAT | O_TRUNC | O_WRONLY;
  if (strcmp(str, "w+") == 0)
    return O_CREAT | O_TRUNC | O_RDWR;
  if (strcmp(str, "a") == 0)
    return O_APPEND | O_CREAT | O_WRONLY;
  if (strcmp(str, "a+") == 0)
    return O_APPEND | O_CREAT | O_RDWR;
  return luaL_error(L, "Unknown file open flag: '%s'", str);
}

/* shamelessly stolen from luvit - bugs are my own */
static void push_stats_table(lua_State* L, struct stat* s) {
  lua_newtable(L);
  lua_pushinteger(L, s->st_dev);
  lua_setfield(L, -2, "dev");
  lua_pushinteger(L, s->st_ino);
  lua_setfield(L, -2, "ino");
  lua_pushinteger(L, s->st_mode);
  lua_setfield(L, -2, "mode");
  lua_pushinteger(L, s->st_nlink);
  lua_setfield(L, -2, "nlink");
  lua_pushinteger(L, s->st_uid);
  lua_setfield(L, -2, "uid");
  lua_pushinteger(L, s->st_gid);
  lua_setfield(L, -2, "gid");
  lua_pushinteger(L, s->st_rdev);
  lua_setfield(L, -2, "rdev");
  lua_pushinteger(L, s->st_size);
  lua_setfield(L, -2, "size");
#ifdef __POSIX__
  lua_pushinteger(L, s->st_blksize);
  lua_setfield(L, -2, "blksize");
  lua_pushinteger(L, s->st_blocks);
  lua_setfield(L, -2, "blocks");
#endif
  lua_pushinteger(L, s->st_atime);
  lua_setfield(L, -2, "atime");
  lua_pushinteger(L, s->st_mtime);
  lua_setfield(L, -2, "mtime");
  lua_pushinteger(L, s->st_ctime);
  lua_setfield(L, -2, "ctime");
#ifndef _WIN32
  lua_pushboolean(L, S_ISREG(s->st_mode));
  lua_setfield(L, -2, "is_file");
  lua_pushboolean(L, S_ISDIR(s->st_mode));
  lua_setfield(L, -2, "is_directory");
  lua_pushboolean(L, S_ISCHR(s->st_mode));
  lua_setfield(L, -2, "is_character_device");
  lua_pushboolean(L, S_ISBLK(s->st_mode));
  lua_setfield(L, -2, "is_block_device");
  lua_pushboolean(L, S_ISFIFO(s->st_mode));
  lua_setfield(L, -2, "is_fifo");
  lua_pushboolean(L, S_ISLNK(s->st_mode));
  lua_setfield(L, -2, "is_symbolic_link");
  lua_pushboolean(L, S_ISSOCK(s->st_mode));
  lua_setfield(L, -2, "is_socket");
#endif
}

/* mostly stolen from luvit - bugs are my own */
static void fs_result(lua_State* L, uv_fs_t* req) {
  if (req->result < 0) {
    lua_pushnil(L);
    lua_pushinteger(L, (uv_errno_t)req->result);
  }
  else {
    switch (req->fs_type) {
      case UV_FS_RENAME:
      case UV_FS_UNLINK:
      case UV_FS_RMDIR:
      case UV_FS_MKDIR:
      case UV_FS_FSYNC:
      case UV_FS_FTRUNCATE:
      case UV_FS_FDATASYNC:
      case UV_FS_LINK:
      case UV_FS_SYMLINK:
      case UV_FS_CHMOD:
      case UV_FS_FCHMOD:
      case UV_FS_CHOWN:
      case UV_FS_FCHOWN:
      case UV_FS_UTIME:
      case UV_FS_FUTIME:
      case UV_FS_CLOSE:
        lua_pushinteger(L, req->result);
        break;

      case UV_FS_OPEN: {
        ray_agent_t* self = ray_agent_new(L);
        luaL_getmetatable(L, "ray.file");
        lua_setmetatable(L, -2);
        self->h.file = req->result;
        break;
      }

      case UV_FS_READ: {
        lua_pushinteger(L, req->result);
        lua_pushlstring(L, (const char*)req->data, req->result);
        free(req->data);
        req->data = NULL;
        break;
      }

      case UV_FS_WRITE:
        lua_pushinteger(L, req->result);
        break;

      case UV_FS_READLINK:
        lua_pushstring(L, (char*)req->ptr);
        break;

      case UV_FS_READDIR: {
        int i;
        char* namep = (char*)req->ptr;
        int   count = req->result;
        lua_newtable(L);
        for (i = 1; i <= count; i++) {
          lua_pushstring(L, namep);
          lua_rawseti(L, -2, i);
          namep += strlen(namep) + 1; /* +1 for '\0' */
        }
        break;
      }

      case UV_FS_STAT:
      case UV_FS_LSTAT:
      case UV_FS_FSTAT:
        push_stats_table(L, (struct stat*)req->ptr);
        break;

      default:
        luaL_error(L, "Unhandled fs_type");
    }
  }
  uv_fs_req_cleanup(req);
}

void ray_fs_cb(uv_fs_t* req) {
  ray_fiber_t* curr = container_of(req, ray_fiber_t, r);
  fs_result(curr->L, req);
  ray_resume(curr, LUA_MULTRET);
}

#define RAY_FS_CALL(L, func, misc, ...) do { \
    ray_fiber_t* curr = ray_current(L); \
    uv_loop_t*   loop = uv_default_loop(); \
    uv_fs_t*     req  = &curr->r.fs; \
    req->data   = misc; \
    uv_fs_cb cb = (curr == RAY_MAIN) ? NULL : ray_fs_cb ; \
    int rc = uv_fs_##func(loop, req, __VA_ARGS__, cb); \
    if (rc < 0) return ray_push_error(L, rc); \
    if (cb) return ray_suspend(curr); \
    else { \
      fs_result(L, req); \
      return lua_gettop(L); \
    } \
  } while(0)

int ray_fs_open(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  int flags = string_to_flags(L, luaL_checkstring(L, 2));
  int mode  = strtoul(luaL_checkstring(L, 3), NULL, 8);
  lua_settop(L, 0);
  RAY_FS_CALL(L, open, NULL, path, flags, mode);
}

int ray_fs_unlink(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, unlink, NULL, path);
}

int ray_fs_mkdir(lua_State* L) {
  const char*  path = luaL_checkstring(L, 1);
  int mode = strtoul(luaL_checkstring(L, 2), NULL, 8);
  lua_settop(L, 0);
  RAY_FS_CALL(L, mkdir, NULL, path, mode);
}

int ray_fs_rmdir(lua_State* L) {
  const char*  path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, rmdir, NULL, path);
}

int ray_fs_readdir(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, readdir, NULL, path, 0);
}

int ray_fs_stat(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, stat, NULL, path);
}

int ray_fs_rename(lua_State* L) {
  const char* old_path = luaL_checkstring(L, 1);
  const char* new_path = luaL_checkstring(L, 2);
  lua_settop(L, 0);
  RAY_FS_CALL(L, rename, NULL, old_path, new_path);
}

int ray_fs_sendfile(lua_State* L) {
  ray_agent_t* o_file = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  ray_agent_t* i_file = (ray_agent_t*)luaL_checkudata(L, 2, "ray.file");
  off_t  ofs = luaL_checkint(L, 3);
  size_t len = luaL_checkint(L, 4);
  lua_settop(L, 2);
  RAY_FS_CALL(L, sendfile, NULL, o_file->h.file, i_file->h.file, ofs, len);
}

int ray_fs_chmod(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  int mode = strtoul(luaL_checkstring(L, 2), NULL, 8);
  lua_settop(L, 0);
  RAY_FS_CALL(L, chmod, NULL, path, mode);
}

int ray_fs_utime(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  double atime = luaL_checknumber(L, 2);
  double mtime = luaL_checknumber(L, 3);
  lua_settop(L, 0);
  RAY_FS_CALL(L, utime, NULL, path, atime, mtime);
}

int ray_fs_lstat(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, lstat, NULL, path);
}

int ray_fs_link(lua_State* L) {
  const char* src_path = luaL_checkstring(L, 1);
  const char* dst_path = luaL_checkstring(L, 2);
  lua_settop(L, 0);
  RAY_FS_CALL(L, link, NULL, src_path, dst_path);
}

int ray_fs_symlink(lua_State* L) {
  const char* src_path = luaL_checkstring(L, 1);
  const char* dst_path = luaL_checkstring(L, 2);
  int flags = string_to_flags(L, luaL_checkstring(L, 3));
  lua_settop(L, 0);
  RAY_FS_CALL(L, symlink, NULL, src_path, dst_path, flags);
}

int ray_fs_readlink(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  lua_settop(L, 0);
  RAY_FS_CALL(L, readlink, NULL, path);
}

int ray_fs_chown(lua_State* L) {
  const char* path = luaL_checkstring(L, 1);
  int uid = luaL_checkint(L, 2);
  int gid = luaL_checkint(L, 3);
  lua_settop(L, 0);
  RAY_FS_CALL(L, chown, NULL, path, uid, gid);
}

int ray_fs_cwd(lua_State* L) {
  char buffer[RAY_MAX_PATH];
  int err = uv_cwd(buffer, RAY_MAX_PATH);
  if (err) {
    return luaL_error(L, uv_strerror(err));
  }
  lua_pushstring(L, buffer);
  return 1;
}

int ray_fs_chdir(lua_State* L) {
  const char* dir = luaL_checkstring(L, 1);
  int err = uv_chdir(dir);
  if (err) {
    return luaL_error(L, uv_strerror(err));
  }
  return 0;
}

int ray_fs_exepath(lua_State* L) {
  char buffer[RAY_MAX_PATH];
  size_t len = RAY_MAX_PATH;
  uv_exepath(buffer, &len);
  lua_pushlstring(L, buffer, len);
  return 1;
}

/* file instance methods */
int ray_file_stat(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  lua_settop(L, 0);
  RAY_FS_CALL(L, fstat, NULL, self->h.file);
}

int ray_file_sync(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  lua_settop(L, 0);
  RAY_FS_CALL(L, fsync, NULL, self->h.file);
}

int ray_file_datasync(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  lua_settop(L, 0);
  RAY_FS_CALL(L, fdatasync, NULL, self->h.file);
}

int ray_file_truncate(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  off_t ofs = luaL_checkint(L, 2);
  lua_settop(L, 0);
  RAY_FS_CALL(L, ftruncate, NULL, self->h.file, ofs);
}

int ray_file_utime(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  double atime = luaL_checknumber(L, 2);
  double mtime = luaL_checknumber(L, 3);
  lua_settop(L, 0);
  RAY_FS_CALL(L, futime, NULL, self->h.file, atime, mtime);
}

int ray_file_chmod(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  int mode = strtoul(luaL_checkstring(L, 2), NULL, 8);
  lua_settop(L, 0);
  RAY_FS_CALL(L, fchmod, NULL, self->h.file, mode);
}

int ray_file_chown(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  int uid = luaL_checkint(L, 2);
  int gid = luaL_checkint(L, 3);
  lua_settop(L, 0);
  RAY_FS_CALL(L, fchown, NULL, self->h.file, uid, gid);
}

int ray_file_read(lua_State *L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");

  size_t  len = luaL_optint(L, 2, RAY_BUF_SIZE);
  int64_t ofs = luaL_optint(L, 3, -1);
  void*   buf = malloc(len); /* free from ctx->r.fs_req.data in cb */

  lua_settop(L, 0);
  RAY_FS_CALL(L, read, buf, self->h.file, buf, len, ofs);
}

int ray_file_write(lua_State *L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");

  size_t   len;
  void*    buf = (void*)luaL_checklstring(L, 2, &len);
  uint64_t ofs = luaL_optint(L, 3, 0);

  lua_settop(L, 0);
  RAY_FS_CALL(L, write, NULL, self->h.file, buf, len, ofs);
}

int ray_file_close(lua_State *L) {
  ray_agent_t* self = (ray_agent_t*)luaL_checkudata(L, 1, "ray.file");
  lua_settop(L, 0);
  RAY_FS_CALL(L, close, NULL, self->h.file);
}

/* ========================================================================== */
/* utils                                                                      */
/* ========================================================================== */
void ray_getaddrinfo_cb(uv_getaddrinfo_t* req, int s, struct addrinfo* ai) {
  ray_fiber_t* self = container_of(req, ray_fiber_t, r);
  char host[INET6_ADDRSTRLEN];
  int  port = 0;

  if (ai->ai_family == PF_INET) {
    struct sockaddr_in* addr = (struct sockaddr_in*)ai->ai_addr;
    uv_ip4_name(addr, host, INET6_ADDRSTRLEN);
    port = addr->sin_port;
  }
  else if (ai->ai_family == PF_INET6) {
    struct sockaddr_in6* addr = (struct sockaddr_in6*)ai->ai_addr;
    uv_ip6_name(addr, host, INET6_ADDRSTRLEN);
    port = addr->sin6_port;
  }

  lua_settop(self->L, 0);

  lua_pushstring (self->L, host);
  lua_pushinteger(self->L, port);

  uv_freeaddrinfo(ai);
  ray_resume(self, 2);
}
int ray_getaddrinfo(lua_State* L) {
  ray_fiber_t* curr = ray_current(L);
  uv_getaddrinfo_t* req = &curr->r.getaddrinfo;

  const char* node      = NULL;
  const char* service   = NULL;
  struct addrinfo hints;

  if (!lua_isnoneornil(L, 1)) {
    node = luaL_checkstring(L, 1);
  }
  if (!lua_isnoneornil(L, 2)) {
    service = luaL_checkstring(L, 2);
  }
  if (node == NULL && service == NULL) {
    return luaL_error(L, "getaddrinfo: provide either node or service");
  }

  hints.ai_family   = PF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags    = 0;

  if (lua_istable(L, 3)) {
    lua_getfield(L, 3, "family");
    if (!lua_isnil(L, -1)) {
      const char* s = lua_tostring(L, -1);
      if (strcmp(s, "INET") == 0) {
        hints.ai_family = PF_INET;
      }
      else if (strcmp(s, "INET6") == 0) {
        hints.ai_family = PF_INET6;
      }
      else {
        return luaL_error(L, "unsupported family: %s", s);
      }
    }
    lua_pop(L, 1);

    lua_getfield(L, 3, "socktype");
    if (!lua_isnil(L, -1)) {
      const char* s = lua_tostring(L, -1);
      if (strcmp(s, "STREAM") == 0) {
        hints.ai_socktype = SOCK_STREAM;
      }
      else if (strcmp(s, "DGRAM")) {
        hints.ai_socktype = SOCK_DGRAM;
      }
      else {
        return luaL_error(L, "unsupported socktype: %s", s);
      }
    }
    lua_pop(L, 1);

    lua_getfield(L, 3, "protocol");
    if (!lua_isnil(L, -1)) {
      const char* s = lua_tostring(L, -1);
      if (strcmp(s, "TCP") == 0) {
        hints.ai_protocol = IPPROTO_TCP;
      }
      else if (strcmp(s, "UDP") == 0) {
        hints.ai_protocol = IPPROTO_UDP;
      }
      else {
        return luaL_error(L, "unsupported protocol: %s", s);
      }
    }
    lua_pop(L, 1);
  }

  uv_loop_t* loop = uv_default_loop();
  int rc = uv_getaddrinfo(loop, req, ray_getaddrinfo_cb, node, service, &hints);
  if (rc) return ray_push_error(L, rc);

  return ray_suspend(curr);
}

int ray_mem_free(lua_State* L) {
  lua_pushinteger(L, uv_get_free_memory());
  return 1;
}

int ray_mem_total(lua_State* L) {
  lua_pushinteger(L, uv_get_total_memory());
  return 1;
}

int ray_hrtime(lua_State* L) {
  lua_pushinteger(L, uv_hrtime());
  return 1;
}

int ray_cpu_info(lua_State* L) {
  int size, i;
  uv_cpu_info_t* info;
  int err = uv_cpu_info(&info, &size);

  lua_settop(L, 0);

  if (err) {
    lua_pushboolean(L, 0);
    luaL_error(L, uv_strerror(err));
    return 2;
  }

  lua_newtable(L);

  for (i = 0; i < size; i++) {
    lua_newtable(L);

    lua_pushstring(L, info[i].model);
    lua_setfield(L, -2, "model");

    lua_pushinteger(L, (lua_Integer)info[i].speed);
    lua_setfield(L, -2, "speed");

    lua_newtable(L); /* times */

    lua_pushinteger(L, (lua_Integer)info[i].cpu_times.user);
    lua_setfield(L, -2, "user");

    lua_pushinteger(L, (lua_Integer)info[i].cpu_times.nice);
    lua_setfield(L, -2, "nice");

    lua_pushinteger(L, (lua_Integer)info[i].cpu_times.sys);
    lua_setfield(L, -2, "sys");

    lua_pushinteger(L, (lua_Integer)info[i].cpu_times.idle);
    lua_setfield(L, -2, "idle");

    lua_pushinteger(L, (lua_Integer)info[i].cpu_times.irq);
    lua_setfield(L, -2, "irq");

    lua_setfield(L, -2, "times");

    lua_rawseti(L, 1, i + 1);
  }

  uv_free_cpu_info(info, size);
  return 1;
}

int ray_interfaces(lua_State* L) {
  int size, i;
  char buf[INET6_ADDRSTRLEN];

  uv_interface_address_t* info;
  int err = uv_interface_addresses(&info, &size);

  lua_settop(L, 0);

  if (err) {
    lua_pushboolean(L, 0);
    luaL_error(L, uv_strerror(err));
    return 2;
  }

  lua_newtable(L);

  for (i = 0; i < size; i++) {
    uv_interface_address_t addr = info[i];

    lua_newtable(L);

    lua_pushstring(L, addr.name);
    lua_setfield(L, -2, "name");

    lua_pushboolean(L, addr.is_internal);
    lua_setfield(L, -2, "is_internal");

    if (addr.address.address4.sin_family == PF_INET) {
      uv_ip4_name(&addr.address.address4, buf, sizeof(buf));
    }
    else if (addr.address.address4.sin_family == PF_INET6) {
      uv_ip6_name(&addr.address.address6, buf, sizeof(buf));
    }

    lua_pushstring(L, buf);
    lua_setfield(L, -2, "address");

    lua_rawseti(L, -2, i + 1);
  }

  uv_free_interface_addresses(info, size);

  return 1;
}

int ray_agent_free(lua_State* L) {
  ray_agent_t* self = (ray_agent_t*)lua_touserdata(L, 1);
  TRACE("FREE: %p\n", self);
  if (self->buf.base) {
    free(self->buf.base);
    self->buf.base = NULL;
    self->buf.size = 0;
    self->buf.offs = 0;
  }
  return 0;
}

static luaL_Reg ray_funcs[] = {
  {"self",      ray_self},
  {"fiber",     ray_fiber_new},
  {"timer",     ray_timer_new},
  {"pipe",      ray_pipe_new},
  {"tcp",       ray_tcp_new},
  {"udp",       ray_udp_new},
  {"open",      ray_fs_open},
  {"unlink",    ray_fs_unlink},
  {"mkdir",     ray_fs_mkdir},
  {"rmdir",     ray_fs_rmdir},
  {"readdir",   ray_fs_readdir},
  {"stat",      ray_fs_stat},
  {"rename",    ray_fs_rename},
  {"sendfile",  ray_fs_sendfile},
  {"chmod",     ray_fs_chmod},
  {"chown",     ray_fs_chown},
  {"utime",     ray_fs_utime},
  {"lstat",     ray_fs_lstat},
  {"link",      ray_fs_link},
  {"symlink",   ray_fs_symlink},
  {"readlink",  ray_fs_readlink},
  {"cwd",       ray_fs_cwd},
  {"chdir",     ray_fs_chdir},
  {"exepath",   ray_fs_exepath},
  {"cpu_info",  ray_cpu_info},
  {"mem_free",  ray_mem_free},
  {"mem_total", ray_mem_total},
  {"hrtime",    ray_hrtime},
  {"interfaces",ray_interfaces},
  {"getaddrinfo",ray_getaddrinfo},
  {"run",       ray_run},
  {NULL,        NULL}
};

static luaL_Reg ray_fiber_meths[] = {
  {"join",      ray_fiber_join},
  {"ready",     ray_fiber_ready},
  {"suspend",   ray_fiber_suspend},
  {NULL,        NULL}
};

static luaL_Reg ray_timer_meths[] = {
  {"close",     ray_close},
  {"start",     ray_timer_start},
  {"stop",      ray_timer_stop},
  {"wait",      ray_timer_wait},
  {"__gc",      ray_agent_free},
  {NULL,        NULL}
};

static luaL_Reg ray_pipe_meths[] = {
  {"read",      ray_read},
  {"write",     ray_write},
  {"close",     ray_close},
  {"listen",    ray_listen},
  {"shutdown",  ray_shutdown},
  {"open",      ray_pipe_open},
  {"accept",    ray_pipe_accept},
  {"bind",      ray_pipe_bind},
  {"connect",   ray_pipe_connect},
  {"__gc",      ray_agent_free},
  {NULL,        NULL}
};

static luaL_Reg ray_tcp_meths[] = {
  {"read",      ray_read},
  {"write",     ray_write},
  {"close",     ray_close},
  {"listen",    ray_listen},
  {"shutdown",  ray_shutdown},
  {"accept",    ray_tcp_accept},
  {"bind",      ray_tcp_bind},
  {"connect",   ray_tcp_connect},
  {"nodelay",   ray_tcp_nodelay},
  {"keepalive", ray_tcp_keepalive},
  {"getpeername", ray_tcp_getpeername},
  {"getsockname", ray_tcp_getsockname},
  {"__gc",      ray_agent_free},
  {NULL,        NULL}
};

static luaL_Reg ray_udp_meths[] = {
  {"send",      ray_udp_send},
  {"recv",      ray_udp_recv},
  {"bind",      ray_udp_bind},
  {"membership",ray_udp_membership},
  {"__gc",      ray_agent_free},
  {NULL,        NULL}
};

static luaL_Reg ray_file_meths[] = {
  {"read",      ray_file_read},
  {"write",     ray_file_write},
  {"close",     ray_file_close},
  {"stat",      ray_file_stat},
  {"sync",      ray_file_sync},
  {"utime",     ray_file_utime},
  {"chmod",     ray_file_chmod},
  {"chown",     ray_file_chown},
  {"datasync",  ray_file_datasync},
  {"truncate",  ray_file_truncate},
  {NULL,        NULL}
};

void ray_pump_cb(uv_async_t* async, int status) {
  (void)async;
  (void)status;
}

LUALIB_API int luaopen_ray(lua_State* L) {

#ifndef WIN32
  signal(SIGPIPE, SIG_IGN);
#endif

  lua_settop(L, 0);

  luaL_newmetatable(L, "ray.fiber");
  luaL_register(L, NULL, ray_fiber_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  luaL_newmetatable(L, "ray.timer");
  luaL_register(L, NULL, ray_timer_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  luaL_newmetatable(L, "ray.pipe");
  luaL_register(L, NULL, ray_pipe_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  luaL_newmetatable(L, "ray.tcp");
  luaL_register(L, NULL, ray_tcp_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  luaL_newmetatable(L, "ray.udp");
  luaL_register(L, NULL, ray_udp_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  luaL_newmetatable(L, "ray.file");
  luaL_register(L, NULL, ray_file_meths);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1);

  uv_async_init(uv_default_loop(), &RAY_PUMP, ray_pump_cb);
  uv_unref((uv_handle_t*)&RAY_PUMP);

  RAY_MAIN = (ray_fiber_t*)malloc(sizeof(ray_fiber_t));
  RAY_MAIN->L   = L;
  RAY_MAIN->ref = LUA_NOREF;

  RAY_MAIN->flags = RAY_STARTED;

  ngx_queue_init(&RAY_MAIN->queue);
  ngx_queue_init(&RAY_MAIN->fiber_queue);

  uv_default_loop()->data = RAY_MAIN;

  luaL_register(L, "ray", ray_funcs);
  return 1;
}


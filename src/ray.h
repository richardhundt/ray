#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>

#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

#ifdef WIN32
# define UNUSED /* empty */
# define INLINE __inline
#else
# define UNUSED __attribute__((unused))
# define INLINE inline
#endif

#undef RAY_DEBUG
#define NGX_DEBUG

#include "uv/include/uv.h"
#include "ngx-queue.h"

#ifdef RAY_DEBUG
#  define TRACE(fmt, ...) do { \
    fprintf(stderr, "%s: %d: %s: " fmt, \
    __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
  } while (0)
#else
#  define TRACE(fmt, ...) ((void)0)
#endif /* RAY_DEBUG */

typedef union ray_handle_u {
  uv_handle_t     handle;
  uv_stream_t     stream;
  uv_tcp_t        tcp;
  uv_pipe_t       pipe;
  uv_prepare_t    prepare;
  uv_check_t      check;
  uv_idle_t       idle;
  uv_async_t      async;
  uv_timer_t      timer;
  uv_fs_event_t   fs_event;
  uv_fs_poll_t    fs_poll;
  uv_poll_t       poll;
  uv_process_t    process;
  uv_tty_t        tty;
  uv_udp_t        udp;
  uv_file         file;
} ray_handle_t;

typedef union ray_req_u {
  uv_req_t          req;
  uv_write_t        write;
  uv_connect_t      connect;
  uv_shutdown_t     shutdown;
  uv_fs_t           fs;
  uv_work_t         work;
  uv_udp_send_t     udp_send;
  uv_getaddrinfo_t  getaddrinfo;
} ray_req_t;

/* default buffer size for read operations */
#define RAY_BUF_SIZE 4096

/* max path length */
#define RAY_MAX_PATH 1024

#define container_of(ptr, type, member) \
  ((type*) ((char*)(ptr) - offsetof(type, member)))

/* lifted from luasys */
#define ray_boxpointer(L,u) \
    (*(void**) (lua_newuserdata(L, sizeof(void*))) = (u))
#define ray_unboxpointer(L,i) \
    (*(void**) (lua_touserdata(L, i)))

/* lifted from luasys */
#define ray_boxinteger(L,n) \
    (*(lua_Integer*) (lua_newuserdata(L, sizeof(lua_Integer))) = (lua_Integer)(n))
#define ray_unboxinteger(L,i) \
    (*(lua_Integer*) (lua_touserdata(L, i)))

#define ray_absindex(L, i) \
  ((i) > 0 || (i) <= LUA_REGISTRYINDEX ? (i) : lua_gettop(L) + (i) + 1)

#define RAY_STARTED 1
#define RAY_CLOSED  2

typedef struct ray_fiber_s  ray_fiber_t;
typedef struct ray_agent_s  ray_agent_t;
typedef struct ray_evt_s    ray_evt_t;
typedef struct ray_evq_s    ray_evq_t;
typedef struct ray_buf_s    ray_buf_t;

struct ray_buf_s {
  size_t  size;
  size_t  offs;
  char*   base;
};

int ray_resume (ray_fiber_t*, int);
int ray_ready  (ray_fiber_t*);
int ray_suspend(ray_fiber_t*);

ray_fiber_t* ray_current(lua_State*);

typedef enum {
  RAY_UNKNOWN = -1,
  RAY_CUSTOM,
  RAY_ERROR,
  RAY_READ,
  RAY_WRITE,
  RAY_CLOSE,
  RAY_CONNECTION,
  RAY_TIMER,
  RAY_IDLE,
  RAY_CONNECT,
  RAY_SHUTDOWN,
  RAY_WORK,
  RAY_FS_CUSTOM,
  RAY_FS_ERROR,
  RAY_FS_OPEN,
  RAY_FS_CLOSE,
  RAY_FS_READ,
  RAY_FS_WRITE,
  RAY_FS_SENDFILE,
  RAY_FS_STAT,
  RAY_FS_LSTAT,
  RAY_FS_FSTAT,
  RAY_FS_FTRUNCATE,
  RAY_FS_UTIME,
  RAY_FS_FUTIME,
  RAY_FS_CHMOD,
  RAY_FS_FCHMOD,
  RAY_FS_FSYNC,
  RAY_FS_FDATASYNC,
  RAY_FS_UNLINK,
  RAY_FS_RMDIR,
  RAY_FS_MKDIR,
  RAY_FS_RENAME,
  RAY_FS_READDIR,
  RAY_FS_LINK,
  RAY_FS_SYMLINK,
  RAY_FS_READLINK,
  RAY_FS_CHOWN,
  RAY_FS_FCHOWN
} ray_type_t;

struct ray_evt_s {
  ray_type_t    type;
  int           info;
  void*         data;
};

struct ray_evq_s {
  size_t        nput;
  size_t        nget;
  size_t        size;
  ray_evt_t*    evts;
};

struct ray_agent_s {
  ray_handle_t  h;
  ray_buf_t     buf;
  int           flags;
  int           count;
  ngx_queue_t   fiber_queue;
  ngx_queue_t   queue;
  void*         data;
  int           ref;
};

struct ray_fiber_s {
  ray_req_t     r;
  lua_State*    L;
  int           L_ref;
  int           flags;
  uv_idle_t     hook;
  ngx_queue_t   fiber_queue;
  ngx_queue_t   queue;
  int           ref;
};

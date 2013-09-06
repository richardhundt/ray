# NAME

ray - simple, coroutine friendly libuv bindings for Lua

# DESCRIPTION

Ray provides:

* TCP and UDP sockets
* filesystem operations
* timers

Oh, and it schedules coroutines instead of using callbacks.

## Example

TCP echo client:

```Lua
local ray = require('ray')
ray.fiber(function()
   local server = ray.tcp()
   server:bind('127.0.0.1', 8080)
   server:listen(128)
   while true do
      local client = server:accept()
      local fib = ray.fiber(function()
         while true do
            local data = client:read()
            if data then
               client:write("you said: "..data)
            else
               client:close()
               break
            end
         end
      end)
   end
end)

ray.run()
```



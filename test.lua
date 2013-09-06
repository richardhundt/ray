local ray = require('ray')
print(ray)

print(ray.open("/tmp/foo.txt", "w+", "0644"))

f1 = ray.fiber(function()
   for i=1, 10 do
      print("f1 tick: ", i)
      ray.yield()
   end
end)

f2 = ray.fiber(function()
   for i=1, 10 do
      print("f2 tick: ", i)
      ray.yield()
   end
end)

f3 = ray.fiber(function()
   local server = ray.tcp()
   print('server:', server)
   print('bind:', server:bind('127.0.0.1', 8080))
   print('listen:', server:listen(128))
   while true do
      print('loop top...')
      local client = server:accept()
      print("client:", client)
      local fib = ray.fiber(function()
         while true do
            local data = client:read()
            print("data:", data)
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

f4 = ray.fiber(function()
   local timer = ray.timer()
   timer:start(1000, 1000)
   while timer:wait() do
      print("tick")
   end
end)

f5 = ray.fiber(function()
   local udp = ray.udp()
   print("udp.bind:", udp:bind('127.0.0.1', 1234))
   for i=1, 10 do
      print('udp.recv:', udp:recv())
   end
end)
f6 = ray.fiber(function()
   local udp = ray.udp()
   for i=1, 10 do
      print('udp.send:', udp:send('127.0.0.1', 1234, "Hello "..tostring(i)))
   end
end)

ray.run()

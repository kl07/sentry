local values = {}
for i=1,#ARGV do
    values[i] = tonumber(redis.call('HGET', KEYS[1], i .. ':' .. ARGV[i])) or 0
end

return math.min(unpack(values))

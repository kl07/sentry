local increment = tonumber(ARGV[1])
assert(increment > 0, 'increment value must be positive')

local values = {}
for i=2,#ARGV do
    local row = i - 1
    local value = tonumber(redis.call('HGET', KEYS[1], row .. ':' .. ARGV[i])) or 0
    values[row] = value
end

local updated = math.min(unpack(values)) + increment
for i=2,#ARGV do
    local row = i - 1
    if updated > values[row] then
        local column = ARGV[i]
        redis.call('HSET', KEYS[1], row .. ':' .. column, updated)
    end
end

return updated

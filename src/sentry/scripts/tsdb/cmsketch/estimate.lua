local sketch = KEYS[1]
local index = KEYS[2]

local index_size = tonumber(ARGV[1])
local value = ARGV[2]
local offset = 3  -- index where bucket variadic argument begins

-- If the index isn't full, we can be sure that all of the frequencies in it
-- are exact counts and not estimates.
local current = nil
local index_usage = redis.call('ZCARD', index)
if index_usage < index_size then
    current = tonumber(redis.call('ZSCORE', index, value)) or 0
else
    local counters = {}
    for i=offset,#ARGV do
        local row = i - offset + 1
        counters[row] = tonumber(redis.call('HGET', sketch, row .. ':' .. ARGV[i])) or 0
    end
    current = math.min(unpack(counters))
end

return current

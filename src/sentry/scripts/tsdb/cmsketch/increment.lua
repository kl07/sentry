local sketch = KEYS[1]
local index = KEYS[2]

local index_size = tonumber(ARGV[1])
local value = ARGV[2]
local increment = tonumber(ARGV[3])
local offset = 4  -- index where bucket variadic argument begins

-- The increment value needs to be positive, since we're using the conservative
-- update strategy proposed by Estan and Varghese:
-- http://www.eecs.harvard.edu/~michaelm/CS223/mice.pdf
assert(increment > 0, 'increment value must be positive')

-- Get the current value of the estimators.
local counters = {}
for i=offset,#ARGV do
    local row = i - offset + 1
    counters[row] = tonumber(redis.call('HGET', sketch, row .. ':' .. ARGV[i])) or 0
end

-- If the index isn't full, we can be sure that all of the frequencies in it
-- are exact counts and not estimates. In that case, we should use that value
-- as the item's current score, since it'll be more accurate than the estimated
-- value.
local current = nil
local index_usage = redis.call('ZCARD', index)
if index_usage < index_size then
    current = tonumber(redis.call('ZSCORE', index, value)) or 0
else
    current = math.min(unpack(counters))
end

-- Update the estimators.
local updated = current + increment
for i=offset,#ARGV do
    local row = i - offset + 1
    if updated > counters[row] then
        local column = ARGV[i]
        redis.call('HSET', KEYS[1], row .. ':' .. column, updated)
    end
end

-- Add (or update) the item's ranking in the index, if we need to.
if index_usage < index_size then
    redis.call('ZADD', index, updated, value)
else
    -- Find the lowest scored item in the index.
    local last = redis.call('ZRANGE', index, 0, 0, 'WITHSCORES')
    -- If the updated value's score is greater than score of the lowest value
    -- in the index, update the score of the updated value, potentially adding
    -- it to the index.
    if updated > tonumber(last[2]) then
        -- The return value of the set addition signifies how many items were
        -- added to the set, so if this item was added, we need to pop off the
        -- last item to maintain the correct index size.
        if redis.call('ZADD', index, updated, value) then
            redis.call('ZREMRANGEBYRANK', index, 0, 0)
        end
    end
end

return updated

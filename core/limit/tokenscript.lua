-- to be compatible with aliyun redis, we cannot use `local key = KEYS[1]` to reuse the key
-- KEYS[1] as tokens_key
-- KEYS[2] as timestamp_key

-- ARGV[1]	rate 「每秒生成几个令牌」
-- ARGV[2]	burst 「令牌桶最大值」
-- ARGV[3]	now_time「当前时间戳」
-- ARGV[4]	get token nums 「开发者需要获取的token数」
-- KEYS[1]	表示资源的tokenkey
-- KEYS[2]	表示刷新时间的key
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- fill_time：需要填满 token_bucket 需要多久
local fill_time = capacity/rate
-- 将填充时间向下取整
local ttl = math.floor(fill_time*2)
local last_tokens = tonumber(redis.call("get", KEYS[1]))

-- 获取目前 token_bucket 中剩余 token 数
-- 如果是第一次进入，则设置 token_bucket 数量为 令牌桶最大值
if last_tokens == nil then
    last_tokens = capacity
end

-- 上一次更新 token_bucket 的时间
local last_refreshed = tonumber(redis.call("get", KEYS[2]))
if last_refreshed == nil then
    last_refreshed = 0
end

local delta = math.max(0, now-last_refreshed)
-- 通过当前时间与上一次更新时间的跨度，以及生产token的速率，计算出新的token数
-- 如果超过 max_burst，多余生产的token会被丢弃
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
    new_tokens = filled_tokens - requested
end

-- 更新新的token数，以及更新时间
redis.call("setex", KEYS[1], ttl, new_tokens)
redis.call("setex", KEYS[2], ttl, now)

return allowed
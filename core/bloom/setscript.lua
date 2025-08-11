-- ARGV:偏移量offset数组
-- KYES[1]: setbit操作的key
-- 全部设置为1
for _, offset in ipairs(ARGV) do
    redis.call("setbit", KEYS[1], offset, 1)
end
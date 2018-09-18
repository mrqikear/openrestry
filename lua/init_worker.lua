--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--


local STATICS_QUEUE_KEY = "statics_queue_key_20180127"
local redis_instance = require 'redis_helper'


local function statics_cache_to_redis(premature)
    if premature then
        return
    end

--    ngx.log(ngx.ERR, "start statics_cache_to_redis!")
    local redis_connection = redis_instance.get_connection()
    if redis_connection == false or redis_connection == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        return
    end

    local shared_lua_conf_dict = ngx.shared.shared_lua_conf_dict
    local count = 1
    redis_connection:init_pipeline()
    while true do
        local pop_queue_key = shared_lua_conf_dict:lpop(STATICS_QUEUE_KEY)
        if pop_queue_key == false or pop_queue_key == nil then
            redis_connection:commit_pipeline()
            break
        end

--        ngx.log(ngx.ERR, "pop key : ", pop_queue_key)
        local cache_value = shared_lua_conf_dict:get(pop_queue_key)
        shared_lua_conf_dict:delete(pop_queue_key)
--        ngx.log(ngx.ERR, "pop value : ", cache_value)
        count = count + 1
        if cache_value ~= false and cache_value ~= nil then
            redis_connection:incrbyfloat(pop_queue_key, cache_value)
            if count > 500 then
                count = 1
                local results, err = redis_connection:commit_pipeline()
                if not results then
                    ngx.log(ngx.ERR, "failed to commit the pipelined requests: ", err)
                end
                redis_connection:init_pipeline()
            end
        end
    end

    redis_connection:set_keepalive(1000, 1000)
--    ngx.log(ngx.ERR, "---end statics_cache_to_redis!")
end

local ok, err = ngx.timer.every(60, statics_cache_to_redis)
if not ok then
    ngx.log(ngx.ERR, "failed to create timer: ", err)
    return
end
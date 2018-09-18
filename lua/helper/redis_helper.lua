--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/9
-- Time: 10:54
--

local cjson = require 'cjson.safe'
local redis = require 'resty.redis'

local _M = {}
local FAILED_QUEUE_NAME = "failed_queue"
-- 20170827：逻辑调整，redis改为短时存储，保鲜时间一小时，确保数据的准确性，不再刷新存储
local EXPIRE_TIMEOUT = 60 * 60
local LONG_EXPIRE_TIMEOUT = 60 * 60 * 24 * 90

local function connect()
    local lua_config = ngx.shared.shared_lua_conf_dict
    local content = lua_config:get('lua_config')
    if not content then
        error("get lua config failed..")
        return false
    end
    local cjson_new = cjson.new()
    local config_table = cjson_new.decode(content)
    local redis_connection, error = redis:new()
    if redis_connection == nil then
        ngx.log(ngx.ERR, "redis:new failed, try once, msg : ", error)
        redis_connection = redis:new()
    end

    for _, one_config in ipairs(config_table['redis_master']) do
        redis_connection:set_timeout(2000)
        local ok, error = redis_connection:connect(one_config['master_host'], one_config['port'])
        if ok then
            if one_config['password'] ~= "" then
                ok, error = redis_connection:auth(one_config['password'])
            end
            if ok then
                return redis_connection
            end
        end
    end
    return false
end

local function connect_read_only()
    local lua_config = ngx.shared.shared_lua_conf_dict
    local content = lua_config:get('lua_config')
    if not content then
        error("get lua config failed..")
        return false
    end
    local cjson_new = cjson.new()
    local config_table = cjson_new.decode(content)
    local redis_connection, error = redis:new()
    if redis_connection == nil then
        ngx.log(ngx.ERR, "redis:new failed, try once, msg : ", error)
        redis_connection = redis:new()
    end

    for _, one_config in ipairs(config_table['redis_read_only']) do
        redis_connection:set_timeout(2000)
        local ok, error = redis_connection:connect(one_config['host'], one_config['port'])
        if ok then
            if one_config['password'] ~= "" then
                ok, error = redis_connection:auth(one_config['password'])
            end
            if ok then
                return redis_connection
            end
        end
    end
    return false
end

function _M.get_connection()
    return connect()
end

function _M.set_and_expire(key, value, expire_time)
    if key == nil or value == nil then
        ngx.log(ngx.ERR, "redis set failed, key : ", key, ", value : ", value)
        return false
    end

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local ok, err = redis:set(key, value)
    if err then
        ngx.log(ngx.ERR, "redis set failed, key : ", key, ", value : ", value, ", info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local ok, err = redis:expire(key, expire_time)
    if err then
        ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", value : ", value, ", info : ", err)
    end
    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return true
end

function _M.check_callback_exist(key)
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        return true
    end
    local result, err = redis:setnx(key, "1")
    if err then
        ngx.log(ngx.ERR, "redis check_callback_exist failed, key : ", key, ", info : ", err)
        return true
    end
    if result == 0 then
        return false
    end

    local ok, err = redis:expire(key, 20)
    if err then
        ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", info : ", err)
    end
    redis:set_keepalive(1000, 1000)
    return true
end

function _M.set(key, value)
    if key == nil or value == nil then
        ngx.log(ngx.ERR, "redis set failed, key : ", key, ", value : ", value)
        return false
    end

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local ok, err = redis:set(key, value)
    if err then
        ngx.log(ngx.ERR, "redis set failed, key : ", key, ", value : ", value, ", info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local ok, err = redis:expire(key, EXPIRE_TIMEOUT)
    if err then
        ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", value : ", value, ", info : ", err)
    end

    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return true
end

function _M.get(key)
    if key == nil then
        ngx.log(ngx.ERR, "redis get failed, key is nil")
        return false
    end

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect_read_only()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local result, err = redis:get(key)
    if err then
        ngx.log(ngx.ERR, "redis get failed, key : ", key, ", info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    if result == ngx.null then
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return nil
    end

--    get操作是否要进行续命?
--    local res, err = redis:ttl(key)
--    if err then
--        ngx.log(ngx.ERR, "redis ttl failed, key : ", key, ", info : ", err)
--        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
--        return false
--    end
--
--    if res < 60 * 60 * 12 then
--        local ok, err = redis:expire(key, EXPIRE_TIMEOUT)
--        if err then
--            ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", value : ", res, ", info : ", err)
--        end
--    end

    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return result
end

function _M.incrby(key, value)
    if key == nil or value == nil then
        ngx.log(ngx.ERR, "redis incrby failed, key : ", key, ", value : ", value)
        return false
    end

    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        return false
    end
    local ok, err = redis:incrbyfloat(key, value)
    if err then
        ngx.log(ngx.ERR, "redis INCRBYFLOAT failed, key : ", key, ", incrby : ", value, ", info : ", err)
        return false
    end

    local ok, err = redis:expire(key, LONG_EXPIRE_TIMEOUT)
    if err then
        ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", value : ", value, ", info : ", err)
    end

    redis:set_keepalive(1000, 1000)
    return true
end

function _M.incr(key)
    if key == nil then
        ngx.log(ngx.ERR, "redis incr failed, key is nil")
        return false
    end

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        return false
    end

    local ok, err = redis:incr(key)
    if err then
        ngx.log(ngx.ERR, "redis incr failed, key : ", key, ", info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end

    local ok, err = redis:expire(key, LONG_EXPIRE_TIMEOUT)
    if err then
        ngx.log(ngx.ERR, "redis expire failed, key : ", key, ", info : ", err)
    end

    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return true
end

function _M.failed_request_insert(record)
    if record == nil then
        ngx.log(ngx.ERR, "redis failed_request_insert failed, record is nil")
        return false
    end

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end
    local ok, err = redis:lpush(FAILED_QUEUE_NAME, record)
    if err then
        ngx.log(ngx.ERR, "redis lpush failed, value : ", record, ", info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end

    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return true
end

function _M.failed_request_pop()

    local redis_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local redis = connect()
    if redis == false or redis == nil then
        ngx.log(ngx.ERR, "redis connected failed")
        return false
    end
    local res, err = redis:lpop(FAILED_QUEUE_NAME)
    if err then
        ngx.log(ngx.ERR, "redis lpop failed, info : ", err)
        ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
        return false
    end

    --    关闭连接操作
    --    local ok, err = redis:close()
    --    if not ok then
    --        ngx.say("failed to close: ", err)
    --        return
    --    end
    redis:set_keepalive(1000, 1000)
    ngx.var.redis_time = ngx.var.redis_time + ngx.now() * 1000 - redis_start_time
    return res
end

return _M
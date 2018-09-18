--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 2015/7/5
-- Time: 10:54
--

local cjson = require 'cjson.safe'
local http = require "http"
local _M = {}
local lua_config = ngx.shared.shared_lua_conf_dict
local config_json = lua_config:get("lua_config")


function _M.http_get_es_affiliate_info(unique_id)
    if unique_id == nil or string.len(unique_id) < 5 then
        return nil
    end
    if config_json == nil then
        ngx.log(ngx.ERR, "redirector.conf empty.")
        return nil
    end

    -- decode and check content
    local cjson_new = cjson.new()
    local content, err = cjson_new.decode(config_json)
    if not content then
        ngx.log(ngx.ERR, "config file json decode error, err : ", err)
        return nil
    end

    local es_start_time = ngx.now() * 1000 -- seconds.milliseconds
    for _, remote_addr in ipairs(content.es_server_list) do
        local httpc = http.new()
        httpc:set_timeout(5000)
        local res, err = httpc:request_uri(remote_addr, {
            body = '{"query":{"bool":{"filter":[{"term":{"unique_id":"' .. unique_id .. '"}}]}}}'
--            body = '{"query":{"bool":{"filter":[{"term":{"unique_id":"' .. unique_id .. '"}}]}},"terminate_after":1,"size":1}'
        })
        if res then
            ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
            local content, err = cjson_new.decode(res.body)
            if content == nil or content["error"] ~= nil then
                ngx.log(ngx.ERR, res.body)
                ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
                return nil
            end
            if content["hits"]["total"] > 0 then
                ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
                for _, query_item in ipairs(content["hits"]["hits"]) do
                    if query_item["_source"]['ads_info'] ~= nil then
                        return query_item
                    end
                end

                return content["hits"]["hits"][1]
            else
                ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
                return nil
            end
            ngx.log(ngx.ERR, " query fail, invalid response body")
        else
            ngx.log(ngx.ERR, remote_addr, ",unique_id:", unique_id, " can't connect, err : ", err)
        end
    end

    ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
    return nil
end

function _M.http_get_es_click(campaign_id, affiliate_id, user_agent, ip_addr)
    if config_json == nil then
        ngx.log(ngx.ERR, "redirector.conf empty.")
        return nil
    end

    -- decode and check content
    local cjson_new = cjson.new()
    local content, err = cjson_new.decode(config_json)
    if not content then
        ngx.log(ngx.ERR, "config file json decode error, err : ", err)
        return nil
    end
    local now = os.time()
    local start_time = os.date("%Y-%m-%dT%H:%M:%S", now - 3600 * 24 * 10)
    local end_time = os.date("%Y-%m-%dT%H:%M:%S", now)
    if user_agent == nil or user_agent == "-" then
        user_agent = "-"
    end

    local es_start_time = ngx.now() * 1000 -- seconds.milliseconds
    for _, remote_addr in ipairs(content.es_server_list) do
        local httpc = http.new()
        httpc:set_timeout(1500)
        local res, err = httpc:request_uri(remote_addr, {
            body = '{"query":{"bool":{"filter":[{"term":{"campaign_id":' .. campaign_id .. '}},{"term":{"remote_addr":"' .. ip_addr ..
                    '"}},{"term":{"http_user_agent":"' .. user_agent .. '"}},{"term":{"affiliate_id":' .. affiliate_id ..
                    '}},{"range":{"@timestamp":{"gt":"' .. start_time .. '","lt":"' .. end_time .. '"}}}]}},"terminate_after":1,"size":0}'
        })
        if res then
            ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
            local content, err = cjson_new.decode(res.body)
            if content == nil or content["error"] ~= nil then
                ngx.log(ngx.ERR, "campaign_id : ", campaign_id, ", ip : ", ip_addr, ", user_agent : ", user_agent,
                    ", affiliate_id : ",affiliate_id, ", start_time : ", start_time, ", end_time : ", end_time,
                    " query es error.")

                ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
                return -1
            end
            if content["hits"]["total"] > 0 then
                ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
                return 0
            end

            ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
            return 1
        else
            ngx.log(ngx.ERR, remote_addr, ",unique_id:", unique_id, " can't connect, err : ", err)
        end
    end

    ngx.var.es_time = ngx.var.es_time + ngx.now() * 1000 - es_start_time
    return -2
end

function _M.http_send_request(url)
    local httpc = http.new()
    httpc:set_timeout(1000)
    local res, err = httpc:request_uri_short(url, {})
    if res ~= nil then
        ngx.var.affiliate_postback_code = res.status
        return true
    else
        ngx.log(ngx.NOTICE, "http_send_request error, msg :" .. err)
        return false
    end
end

function _M.http_get(url)
    if config_json == nil then
        ngx.log(ngx.ERR, "redirector.conf empty.\\n")
        return nil
    end

    -- decode and check content
    local cjson_new = cjson.new()
    local content, err = cjson_new.decode(config_json)
    if not content then
        ngx.log(ngx.ERR, "config file json decode error.\\n")
        return nil
    end

    for _, remote_addr in ipairs(content.controller_list) do
        local httpc = http.new()
        httpc:set_timeout(1500)
        local res, err = httpc:request_uri("http://" .. remote_addr .. url, {})
        if res then
            local content, err = cjson_new.decode(res.body)
            if content then
                return content
            end
            ngx.log(ngx.ERR, "update fail, invalid response body: ", err)
        else
            ngx.log(ngx.ERR, remote_addr, " can't connect.")
        end
    end
    return nil
end

return _M
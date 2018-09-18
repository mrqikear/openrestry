--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/9
-- Time: 10:54
--

local mysql = require 'resty.mysql'
local cjson = require 'cjson.safe'

local _M = {}

local function connect()
    local lua_config = ngx.shared.shared_lua_conf_dict
    local content = lua_config:get('lua_config')
    if not content then
        error("get lua config failed..")
        return false
    end
    local cjson_new = cjson.new()
    local config_table = cjson_new.decode(content)
    local error = nil

    for _, one_config in ipairs(config_table['mysql']) do
        local mysql_connection, error = mysql:new()
        if not mysql_connection then
            ngx.log(ngx.ERR, "failed to instantiate mysql : ", error)
            return false
        end

        mysql_connection:set_timeout(2000)
        local ok, error, errorno, salstate = mysql_connection:connect {
            host = one_config['host'],
            port = one_config['port'],
            database = one_config['db'],
            user = one_config['user'],
            password = one_config['password'],
            max_packet_size = 1024 * 1024
        }
        if ok then
            local times, err = mysql_connection:get_reused_times()
            if times == 0 then
                mysql_connection:query("SET NAMES 'utf8';");
            end
            return mysql_connection
        end
        ngx.log(ngx.ERR, "failed to connect : ", error, " : ", errorno, " ", salstate)
    end

    ngx.log(ngx.ERR, "none mysql database can use.")
    return false
end

function _M.get_offer_info(campaign_id)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end
    local res_table, error, errorno, sqlstate = mysql_connection:query("select ads_price,affiliate_price,total_cap," ..
            "daily_cap,start_time,end_time,status,adviser_id,target_geo,campaign_name,adv_tracking_link,click_status," ..
            "auto_ads,auto_ads_id,auto_campaign_id,conversion_flow from `campaign` where id ='" .. campaign_id .. "'")
    if not res_table then
        ngx.log(ngx.ERR, "get_offer_info bad result: ", error, ": ", errorno, ": ", sqlstate, ".")
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    if #res_table == 0 then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return res_table[1]
end

function _M.get_user_default_postback_url(affiliate_id)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end
    local res_table, error, errorno, sqlstate = mysql_connection:query("select default_postback_url from `user`" ..
            " where id ='" .. affiliate_id .. "'")
    if not res_table then
        ngx.log(ngx.ERR, "get_offer_info bad result: ", error, ": ", errorno, ": ", sqlstate, ".")
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    if #res_table == 0 then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return res_table[1]['default_postback_url']
end

function _M.extract_campaign_id(id)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return id
    end
    local res_table, error, errorno, sqlstate = mysql_connection:query("select id from " ..
            "`campaign` where visible=1 and outer_campaign_id='" .. id .. "'")
    if not res_table then
        ngx.log(ngx.ERR, "get_offer_info bad result: ", error, ": ", errorno, ": ", sqlstate, ".")
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return id
    end

    if #res_table ~= 0 then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return res_table[1]['id']
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return id

end

function _M.get_affiliate_offer_info(campaign_id, affiliate_id)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end
    local res_table, error, errorno, sqlstate = mysql_connection:query("select id,campaign_id,affiliate_user_id," ..
            "postback_url,decide_to_postback,affiliate_price,total_cap,daily_cap,start_time,end_time,status,deduction_rate from " ..
            "`running_offer` where campaign_id ='" .. campaign_id .. "' and affiliate_user_id = '" .. affiliate_id .. "'")
    if not res_table then
        ngx.log(ngx.ERR, "get_affiliate_offer_info bad result: ", error, ": ", errorno, ": ", sqlstate, ".")
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    if #res_table == 0 then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return res_table[1]
end

function _M.get_low_level_smart_link_info(campaign_id, affiliate_id, subpub)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end
    local query_sql = "SELECT inner_campaign_id,priority FROM " ..
            "cloudmob_campaign_control.smart_link WHERE (affiliate_id='" .. affiliate_id .. "' or affiliate_id is NULL or affiliate_id='') " ..
            "AND (campaign_id='" .. campaign_id .. "' OR campaign_id is NULL or campaign_id='') AND (affiliate_callback_subpub='" ..
            subpub .. "' OR affiliate_callback_subpub is NULL or affiliate_callback_subpub='') AND status=2 and priority<50 ORDER BY priority DESC limit 5;"
    local res_table, error, errorno, sqlstate = mysql_connection:query(query_sql)
    if not res_table then
        ngx.log(ngx.ERR, "get_smart_link bad result: ", error, ": ", errorno, ": ", sqlstate, ",sql :" .. query_sql)
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    if #res_table == 0 then
--        ngx.log(ngx.ERR, "get_low_level_smart_link_info sql :" .. query_sql)
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return res_table
end

function _M.get_high_level_smart_link_info(campaign_id, affiliate_id, subpub)
    local mysql_start_time = ngx.now() * 1000 -- seconds.milliseconds
    local mysql_connection = connect()
    if mysql_connection == false then
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end
    local query_sql = "SELECT inner_campaign_id,priority FROM " ..
            "cloudmob_campaign_control.smart_link WHERE (affiliate_id='" .. affiliate_id .. "' or affiliate_id is NULL or affiliate_id='') " ..
            "AND (campaign_id='" .. campaign_id .. "' OR campaign_id is NULL or campaign_id='') AND (affiliate_callback_subpub='" ..
            subpub .. "' OR affiliate_callback_subpub is NULL or affiliate_callback_subpub='') AND status=2 and priority>50 ORDER BY priority DESC limit 5;"
    local res_table, error, errorno, sqlstate = mysql_connection:query(query_sql)
    if not res_table then
        ngx.log(ngx.ERR, "get_smart_link bad result: ", error, ": ", errorno, ": ", sqlstate, ",sql :" .. query_sql)
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    local ok, error = mysql_connection:set_keepalive(1000, 1000)
    if not ok then
        ngx.log(ngx.WARN, "failed to set keepalive : ", error)
    end

    if #res_table == 0 then
--        ngx.log(ngx.ERR, "get_high_level_smart_link_info sql :" .. query_sql)
        ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
        return false
    end

    ngx.var.mysql_time = ngx.var.mysql_time + ngx.now() * 1000 - mysql_start_time
    return res_table
end

return _M
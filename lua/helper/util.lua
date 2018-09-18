--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--

local _M = {}

local redis_instance = require 'redis_helper'
local mysql_instance = require 'mysql_helper'
local cjson = require 'cjson.safe'
local STATICS_QUEUE_KEY = "statics_queue_key_20180127"

function _M.lua_string_split(str, split_char)
    local sub_str_tab = {};
    local i = 0;
    local j = 0;
    while true do
        j = string.find(str, split_char, i + 1); -- 从目标串str第i+1个字符开始搜索指定串
        if j == nil then
            table.insert(sub_str_tab, str);
            break;
        end;
        table.insert(sub_str_tab, string.sub(str, i + 1, j - 1));
        i = j;
    end
    return sub_str_tab;
end


-- subpub混淆，字符串转ascii_，并添加首位混淆数字
function _M.subpusb_mix(subpub_ori)
    local str_len = string.len(subpub_ori)
    local mix_head_num = 7
    local mix_tail_num = 7
    for i=1, str_len do
        local ascii_num = string.byte(subpub_ori,i)
        mix_head_num  = mix_head_num * ascii_num
        mix_tail_num  = mix_tail_num + ascii_num
    end
    mix_head_num = mix_head_num % 10
    mix_tail_num = mix_tail_num % 10
    if string.find(subpub_ori, "SUBID") ~= nil then
        subpub_ori = string.gsub(subpub_ori, "SUBID", "0107." .. mix_head_num)
    else
        subpub_ori = mix_head_num .. subpub_ori
    end
    return string.format("%s%d", subpub_ori, mix_tail_num)
end


local function extract_user_offer_simple(user_offer_detail_table)
    user_offer_detail_table["status"] = tonumber(user_offer_detail_table["status"])
    if user_offer_detail_table["daily_cap"] == nil then
        user_offer_detail_table["daily_cap"] = 0;
    else
        user_offer_detail_table["daily_cap"] = tonumber(user_offer_detail_table["daily_cap"])
    end
    if user_offer_detail_table["total_cap"] == nil then
        user_offer_detail_table["total_cap"] = 0
    else
        user_offer_detail_table["total_cap"] = tonumber(user_offer_detail_table["total_cap"])
    end
    if user_offer_detail_table["deduction_rate"] == nil then
        user_offer_detail_table["deduction_rate"] = 0
    else
        user_offer_detail_table["deduction_rate"] = tonumber(user_offer_detail_table["deduction_rate"])
    end
    user_offer_detail_table["affiliate_price"] = tonumber(user_offer_detail_table["affiliate_price"])
    user_offer_detail_table["start_time"] = tonumber(user_offer_detail_table["start_time"])
    user_offer_detail_table["end_time"] = tonumber(user_offer_detail_table["end_time"])
    user_offer_detail_table["affiliate_user_id"] = tostring(user_offer_detail_table["affiliate_user_id"])
    user_offer_detail_table["campaign_id"] = tostring(user_offer_detail_table["campaign_id"])
    user_offer_detail_table["decide_to_postback"] = tonumber(user_offer_detail_table["decide_to_postback"])

    return user_offer_detail_table
end


local function extract_ads_offer_simple(ads_offer_detail_table)
    ads_offer_detail_table["affiliate_price"] = tonumber(ads_offer_detail_table["affiliate_price"])
    ads_offer_detail_table["start_time"] = tonumber(ads_offer_detail_table["start_time"])
    ads_offer_detail_table["end_time"] = tonumber(ads_offer_detail_table["end_time"])
    ads_offer_detail_table["ads_price"] = tonumber(ads_offer_detail_table["ads_price"])
    ads_offer_detail_table["affiliate_price"] = tonumber(ads_offer_detail_table["affiliate_price"])
    ads_offer_detail_table["adviser_id"] = tostring(ads_offer_detail_table["adviser_id"])
    ads_offer_detail_table["level"] = tonumber(ads_offer_detail_table["level"])
    ads_offer_detail_table["status"] = tonumber(ads_offer_detail_table["status"])
    ads_offer_detail_table["auto_ads"] = tonumber(ads_offer_detail_table["auto_ads"])

    if ads_offer_detail_table["daily_cap"] == nil then
        ads_offer_detail_table["daily_cap"] = 0
    else
        ads_offer_detail_table["daily_cap"] = tonumber(ads_offer_detail_table["daily_cap"])
    end
    if ads_offer_detail_table["total_cap"] == nil then
        ads_offer_detail_table["total_cap"] = 0
    else
        ads_offer_detail_table["total_cap"] = tonumber(ads_offer_detail_table["total_cap"])
    end
    return ads_offer_detail_table
end

-- decode identifier => base64(timestamp[10bit]userid-offerid)
function _M.get_userid_and_offerid(affiliate_base64)
    affiliate_base64 = string.gsub(affiliate_base64, '%%(%x%x)', function(h) return string.char(tonumber(h, 16)) end)
    local affiliate_origin = ngx.decode_base64(affiliate_base64)
    if affiliate_origin == nil then
        return nil, nil
    end

    local pos = string.find(affiliate_origin, "-")
    if pos == nil then
        return nil, nil
    end

    if string.len(affiliate_origin) < 10 then
        return nil, nil
    end
    local user_id = string.sub(affiliate_origin, 11, pos - 1)
    local campaign_id = string.sub(affiliate_origin, pos + 1)

    --    -- 检索redis，查找出目前对应的真正的campaign_id，当redis挂了时，查询数据库，并更新redis（20170826这里是之前的outer_campaign_id显示时的方式，已经废弃，目前outer_campaign_id只用于去重）
    --    local outer_campaign_id = string.sub(affiliate_origin, pos + 1)
    --    local campaign_id = _M.redis_get_shell(redis_instance.get, "cache-outer_campaign_id-map-" .. outer_campaign_id, 1)
    --    if campaign_id == nil or campaign_id == false then
    --        campaign_id = mysql_instance.extract_campaign_id(outer_campaign_id)
    --        pcall(redis_instance.set_and_expire, "cache-outer_campaign_id-map-" .. outer_campaign_id, campaign_id, 60 * 4)
    --    end

    return user_id, campaign_id
end


function _M.through_proxy()
    local remote_address = ngx.var.http_x_forwarded_for
    if remote_address == nil then
        remote_address = ngx.var.remote_addr
        return remote_address
    end

    local doc_pos = string.find(remote_address, ",")
    local times = 0
    while doc_pos ~= nil and times < 5 do
        remote_address = string.sub(remote_address, doc_pos + 1)
        times = times + 1
    end
    if times >= 5 then
        remote_address = ngx.var.remote_addr
    end

    return remote_address
end

function _M.local_cache_statics(statics_key, statics_value)
    local shared_lua_conf_dict = ngx.shared.shared_lua_conf_dict
    local cache_value = shared_lua_conf_dict:get(statics_key)

    if cache_value == nil then
        shared_lua_conf_dict:rpush(STATICS_QUEUE_KEY, statics_key)
        shared_lua_conf_dict:set(statics_key, statics_value)
        return
    end

    shared_lua_conf_dict:incr(statics_key, statics_value)
    return
end

function _M.redis_get_shell(method, param, times)
    local ok, msg = pcall(method, param)
    if ok then
        return msg
    end

    if times > 2 then
        ngx.log(ngx.ERR, "redis call error happened, try_times : ", times, " --msg : ", msg)
        return false
    end

    times = times + 1
    _M.redis_get_shell(method, param, times)
end


function _M.get_cache(user_id, campaign_id)
    local shared_lua_conf_dict = ngx.shared.shared_lua_conf_dict
    local cache_campaign_id_key = "cache-campaign-id-" .. campaign_id
    local cache_running_offer_id_key = "cache-affiliate-id-" .. user_id .. "-offer-id-" .. campaign_id

    -- local cache first and then redis cache.
    local ads_offer_info_str = shared_lua_conf_dict:get(cache_campaign_id_key)
    local user_offer_info_str = shared_lua_conf_dict:get(cache_running_offer_id_key)

    if ads_offer_info_str == nil or ads_offer_info_str == false then
        ads_offer_info_str = _M.redis_get_shell(redis_instance.get, cache_campaign_id_key, 1)
        shared_lua_conf_dict:set(cache_campaign_id_key, ads_offer_info_str, 5)
    end
    if user_offer_info_str == nil or user_offer_info_str == false then
        user_offer_info_str = _M.redis_get_shell(redis_instance.get, cache_running_offer_id_key, 1)
        shared_lua_conf_dict:set(cache_running_offer_id_key, user_offer_info_str, 5)
    end

    local user_offer_info_table, ads_offer_info_table = nil
    local cjson_new = cjson.new()

    -- if no redis cache, visit db and push to redis.
    if ads_offer_info_str == nil or ads_offer_info_str == false then
        ads_offer_info_table = mysql_instance.get_offer_info(campaign_id)
        if ads_offer_info_table == false then
            ngx.var.error_msg = "mysql get offer info failed(not exist), campaign_id : " .. campaign_id
            ngx.log(ngx.ERR, ngx.var.error_msg)
            return false, false
        end
        ads_offer_info_str = cjson_new.encode(extract_ads_offer_simple(ads_offer_info_table))
        pcall(redis_instance.set, cache_campaign_id_key, ads_offer_info_str)
        shared_lua_conf_dict:set(cache_campaign_id_key, ads_offer_info_str, 5)
    else
        ads_offer_info_table = cjson_new.decode(ads_offer_info_str)
    end

    if user_offer_info_str == nil or user_offer_info_str == false then
        user_offer_info_table = mysql_instance.get_affiliate_offer_info(campaign_id, user_id)
        if user_offer_info_table == false then
            ngx.var.error_msg = "mysql get affiliate offer info failed(not exist), campaign_id : " .. campaign_id .. ", user_id : " .. user_id
            ngx.log(ngx.ERR, ngx.var.error_msg)
            return ads_offer_info_table, false
        end
        user_offer_info_str = cjson_new.encode(extract_user_offer_simple(user_offer_info_table))
        pcall(redis_instance.set, cache_running_offer_id_key, user_offer_info_str)
        shared_lua_conf_dict:set(cache_running_offer_id_key, user_offer_info_str, 5)
    else
        user_offer_info_table = cjson_new.decode(user_offer_info_str)
    end

    return ads_offer_info_table, user_offer_info_table
end

function _M.get_low_smart_link(campaign_id, user_id, subpub)
    local shared_lua_conf_dict = ngx.shared.shared_lua_conf_dict
    local smart_link_cache_key = "smart_link_low_cache_" .. campaign_id .. "_" .. user_id .. "_" .. subpub
    local smart_link_info_str = shared_lua_conf_dict:get(smart_link_cache_key)
    if smart_link_info_str == nil or smart_link_info_str == false then
        smart_link_info_str = _M.redis_get_shell(redis_instance.get, smart_link_cache_key, 1)
        shared_lua_conf_dict:set(smart_link_cache_key, smart_link_info_str, 20)
    end

    local smart_link_inf_list = false
    local cjson_new = cjson.new()
    if smart_link_info_str == nil or smart_link_info_str == false then
        smart_link_inf_list = mysql_instance.get_low_level_smart_link_info(campaign_id, user_id, subpub)
        if smart_link_inf_list == false or smart_link_inf_list == nil then
--            ngx.var.error_msg = "mysql get_low_smart_link failed"
--            ngx.log(ngx.ERR, ngx.var.error_msg)
            return false
        end
        smart_link_info_str = cjson_new.encode(smart_link_inf_list)
        pcall(redis_instance.set_and_expire, smart_link_cache_key, smart_link_info_str, 3600 * 6)
    else
        smart_link_inf_list = cjson_new.decode(smart_link_info_str)
    end

    return smart_link_inf_list
end

function _M.get_high_smart_link(campaign_id, user_id, subpub)
    local shared_lua_conf_dict = ngx.shared.shared_lua_conf_dict
    local smart_link_cache_key = "smart_link_high_cache_" .. campaign_id .. "_" .. user_id .. "_" .. subpub
    local smart_link_info_str = shared_lua_conf_dict:get(smart_link_cache_key)
    if smart_link_info_str == nil or smart_link_info_str == false then
        smart_link_info_str = _M.redis_get_shell(redis_instance.get, smart_link_cache_key, 1)
        shared_lua_conf_dict:set(smart_link_cache_key, smart_link_info_str, 20)
    end

    local smart_link_inf_list = false
    local cjson_new = cjson.new()
    if smart_link_info_str == nil or smart_link_info_str == false then
        smart_link_inf_list = mysql_instance.get_high_level_smart_link_info(campaign_id, user_id, subpub)
        if smart_link_inf_list == false or smart_link_inf_list == nil then
--            ngx.var.error_msg = "mysql get_high_smart_link failed"
--            ngx.log(ngx.ERR, ngx.var.error_msg)
            return false
        end
        smart_link_info_str = cjson_new.encode(smart_link_inf_list)
        pcall(redis_instance.set_and_expire, smart_link_cache_key, smart_link_info_str, 3600 * 6)
    else
        smart_link_inf_list = cjson_new.decode(smart_link_info_str)
    end

    return smart_link_inf_list
end

function _M.extract_number(table, item)
    if item == nil then
        table = tonumber(table)
        if table == nil or table == false then
            return 0
        else
            return table
        end
    end

    local data = tonumber(table[item])
    if data == nil or data == false then
        return 0
    end
    return data
end

function _M.check_affiliate_view_cap_and_status(user_offer_info_table)
    local running_offer_id = _M.extract_number(user_offer_info_table, "id")
    local affiliate_daily_cap = _M.extract_number(user_offer_info_table, "daily_cap")
    local affiliate_total_cap = _M.extract_number(user_offer_info_table, "total_cap")
    --    local affiliate_start_time = extract_number(user_offer_info_table, "start_time")
    --    local affiliate_end_time = extract_number(user_offer_info_table, "end_time")
    local current_affiliate_daily_cap = _M.extract_number(redis_instance.get("daily_cap-affiliate-running_offer-" .. running_offer_id), nil)
    local current_affiliate_total_cap = _M.extract_number(redis_instance.get("total_cap-affiliate-running_offer-" .. running_offer_id), nil)

    if user_offer_info_table["status"] ~= 2 then
        ngx.var.error_msg = "check_affiliate_view_cap_and_status status not running(2), running_offer_id : " .. running_offer_id
        return false
    end

    -- 累计cap,根据cap,time 等判断是否需要进行回传
    -- current_timestamp <= affiliate_start_time or current_timestamp >= affiliate_end_time then 暂时affiliate 维度的时间未投入使用
    if (affiliate_total_cap ~= 0 and current_affiliate_total_cap >= affiliate_total_cap) or
            (affiliate_daily_cap ~= 0 and current_affiliate_daily_cap >= affiliate_daily_cap) then
        ngx.var.error_msg = "check_affiliate_view_cap_and_status no cap remaining, running_offer_id : " .. running_offer_id
        return false
    end

    return true
end

function _M.check_ads_view_cap_and_expire_time_and_status(ads_offer_info_table, campaign_id, current_timestamp)
    local ads_daily_cap = _M.extract_number(ads_offer_info_table, 'daily_cap')
    local ads_total_cap = _M.extract_number(ads_offer_info_table, 'total_cap')
    local ads_start_time = _M.extract_number(ads_offer_info_table, "start_time")
    local ads_end_time = _M.extract_number(ads_offer_info_table, "end_time")
    local current_offer_daily_cap = _M.extract_number(_M.redis_get_shell(redis_instance.get, "daily_cap-ads-" .. campaign_id), nil)
    local current_offer_total_cap = _M.extract_number(_M.redis_get_shell(redis_instance.get, "total_cap-ads—" .. campaign_id), nil)

    if (ads_total_cap ~= 0 and current_offer_total_cap >= ads_total_cap) or
            (ads_daily_cap ~= 0 and current_offer_daily_cap >= ads_daily_cap) then
        ngx.var.error_msg = "check_ads_view_cap_and_expire_time_and_status no cap remaning, campaign_id : " .. campaign_id
        return false
    end

    if current_timestamp < ads_start_time or current_timestamp > ads_end_time then
        ngx.var.error_msg = "check_ads_view_cap_and_expire_time_and_status not valid time, campaign_id : " .. campaign_id
        return false
    end

    if ads_offer_info_table["status"] ~= 2 then
        ngx.var.error_msg = "check_ads_view_cap_and_expire_time_and_status status not running(2), campaign_id : " .. campaign_id
        return false
    end

    -- get ads tracking link and construct params
    local ads_tracking_link = ads_offer_info_table["adv_tracking_link"]
    if ads_tracking_link == nil then
        ngx.var.error_msg = "check_ads_view_cap_and_expire_time_and_status ads_tracking_link is nil, campaign_id : " .. campaign_id
        return false
    end

    return true
end

return _M


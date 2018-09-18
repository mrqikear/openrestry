--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--

local cjson = require 'cjson.safe'
local iplib = require 'iplib'
local util = require 'util'
local resty_random = require 'resty.random'
local http_helper = require "http_helper"
local request_uri = ngx.var.request_uri


local function process_http_request()
    local current_timestamp = ngx.now() -- seconds.milliseconds0
    local cjson_new = cjson.new()
    local affiliate_link_unique_md5 = ngx.md5(request_uri .. current_timestamp .. resty_random.bytes(16, true))
    local remote_address = util.through_proxy()
    ngx.var.remote_address = remote_address
    local ip_info = iplib.locate(remote_address)
    ngx.var.decide_to_postback = "true"

    -- get geo info.
    if not ip_info then
        ngx.log(ngx.ERR, "remote address : " .. remote_address .. " can't find in iplib.")
        ngx.var.rs_geo = remote_address
    else
        ngx.var.rs_geo = ip_info['acl']
    end

    -- get params and affiliate id : timestamp[10bit]userid-offerid
    local args_bale = {}
    local subpub = ngx.var.arg_subpub
    local identify = ngx.var.arg_id
    local aff_sub1 = ngx.var.arg_aff_sub1
    local aff_sub2 = ngx.var.arg_aff_sub2
    local aff_sub3 = ngx.var.arg_aff_sub3
    local aff_sub4 = ngx.var.arg_aff_sub4
    local aff_sub5 = ngx.var.arg_aff_sub5
    local aff_sub6 = ngx.var.arg_aff_sub6
    local aff_sub7 = ngx.var.arg_aff_sub7
    local aff_sub8 = ngx.var.arg_aff_sub8

    if subpub == nil then
        subpub = "_"
    else
        args_bale["subpub"] = subpub
    end
    if aff_sub1 == nil then
        aff_sub1 = "_"
    else
        args_bale["aff_sub1"] = aff_sub1
    end
    if aff_sub2 == nil then
        aff_sub2 = "_"
    else
        args_bale["aff_sub2"] = aff_sub2
    end
    if aff_sub3 == nil then
        aff_sub3 = "_"
    else
        args_bale["aff_sub3"] = aff_sub3
    end
    if aff_sub4 == nil then
        aff_sub4 = "_"
    else
        args_bale["aff_sub4"] = aff_sub4
    end
    if aff_sub5 == nil then
        aff_sub5 = "_"
    else
        args_bale["aff_sub5"] = aff_sub5
    end
    if aff_sub6 == nil then
        aff_sub6 = "_"
    else
        args_bale["aff_sub6"] = aff_sub6
    end
    if aff_sub7 == nil then
        aff_sub7 = "_"
    else
        args_bale["aff_sub7"] = aff_sub7
    end
    if aff_sub8 == nil then
        aff_sub8 = "_"
    else
        args_bale["aff_sub8"] = aff_sub8
    end
    if identify == nil then
        ngx.var.result_code = 1001
        return false
    end

    -- init log info
    ngx.var.unique_id = affiliate_link_unique_md5
    args_bale["id"] = identify
    ngx.var.request_params = cjson_new.encode(args_bale)

    local user_id, campaign_id = util.get_userid_and_offerid(identify)
    if user_id == nil or campaign_id == nil then
        ngx.var.error_msg = "affiliate id parse error, id : " .. identify
        ngx.var.result_code = 1002
        return false
    end

    ngx.var.affiliate_id = user_id
    ngx.var.campaign_id = campaign_id

    -- 因为冒号:影响后面的拼接处理故在这将user_agent中的冒号统一转换成下划线_进行处理
    local http_user_agent = ngx.var.http_user_agent
    if http_user_agent == nil then
        http_user_agent = "-"
    end
    http_user_agent = string.gsub(http_user_agent, "\"", "_")
    ngx.var.local_user_agent = http_user_agent
    local is_unique_click = 1--http_helper.http_get_es_click(campaign_id, user_id, http_user_agent, remote_address)
    ngx.var.is_unique_click = is_unique_click

    -- 获取smart_link信息
    local high_level_campaign_list = util.get_high_smart_link(campaign_id, user_id, subpub)
    local low_level_campaign_list = 0
    -- get redis cache and check
    local ads_offer_info_table = true
    local user_offer_info_table = true
    local high_level_count = 1
    local low_level_count = 1
    while true do
        if high_level_campaign_list ~= false and high_level_campaign_list ~= nil and high_level_count <= #high_level_campaign_list then
            campaign_id = high_level_campaign_list[high_level_count]["inner_campaign_id"]
            ngx.var.decide_to_postback = "false"
            ngx.var.result_code = 1111
        end

        ads_offer_info_table, user_offer_info_table = util.get_cache(user_id, campaign_id)
        if user_offer_info_table ~= false and not util.check_affiliate_view_cap_and_status(user_offer_info_table) then
            ngx.var.decide_to_postback = "false"
            ngx.var.result_code = 1112
        end
        if ads_offer_info_table ~= false and util.check_ads_view_cap_and_expire_time_and_status(ads_offer_info_table, campaign_id, current_timestamp) then
            break
        end

        high_level_count = high_level_count + 1
        -- try low level smart link info
        if (high_level_campaign_list == false or high_level_count > #high_level_campaign_list) and low_level_count == 1 then
            low_level_campaign_list = util.get_low_smart_link(campaign_id, user_id, subpub)
        end
        if (high_level_campaign_list == false or high_level_count > #high_level_campaign_list) and
                low_level_campaign_list ~= false and low_level_campaign_list ~= nil and low_level_count <= #low_level_campaign_list then
            campaign_id = low_level_campaign_list[low_level_count]["inner_campaign_id"]
            ngx.var.decide_to_postback = "false"
            ngx.var.result_code = 1111
        end
        if low_level_campaign_list ~= 0 and (low_level_campaign_list == false or low_level_count > #low_level_campaign_list) then
            -- 各种smart-link都没用命中的情况下，还是才用默认的回传链接
            campaign_id = ngx.var.campaign_id
            ads_offer_info_table, user_offer_info_table = util.get_cache(user_id, campaign_id)
            ngx.var.result_code = 1113
            break
        end
        low_level_count = low_level_count +  1
    end

    ngx.var.smart_campaign_id = campaign_id
    if ads_offer_info_table == nil or ads_offer_info_table == false then
        ngx.var.error_msg = "can't find ads_offer_info_table by using campaign id : " .. campaign_id
        ngx.var.result_code = 1003
        return false
    end

--    ngx.var.campaign_name = ads_offer_info_table['campaign_name']
    -- 计入点击数据进入redis
    local statics_key = "statics_click_cache-" .. user_id .. "-" .. ngx.var.campaign_id
            .. "-" .. ads_offer_info_table['adviser_id'] .. "-" .. campaign_id
    util.local_cache_statics(statics_key, 1)
--    unique-click 目前已经不使用，所以不统计了。
--    if is_unique_click == 1 then
--        util.local_cache_statics(statics_key, 1)
--    end

    -- prepare log info
    local ads_tracking_link = ads_offer_info_table["adv_tracking_link"]
--    ads_tracking_link = string.gsub(ads_tracking_link, "%%", "%%%%")
    subpub = util.subpusb_mix(subpub)
    subpub = string.gsub(subpub, "%%", "%%%%")
    aff_sub1 = string.gsub(aff_sub1, "%%", "%%%%")
    aff_sub2 = string.gsub(aff_sub2, "%%", "%%%%")
    aff_sub3 = string.gsub(aff_sub3, "%%", "%%%%")
    aff_sub4 = string.gsub(aff_sub4, "%%", "%%%%")
    aff_sub5 = string.gsub(aff_sub5, "%%", "%%%%")
    aff_sub6 = string.gsub(aff_sub6, "%%", "%%%%")
    aff_sub7 = string.gsub(aff_sub7, "%%", "%%%%")
    aff_sub8 = string.gsub(aff_sub8, "%%", "%%%%")
    ads_tracking_link = string.gsub(ads_tracking_link, "{subpub}", user_id .. '.' .. subpub)
    ads_tracking_link = string.gsub(ads_tracking_link, "{click_id}", affiliate_link_unique_md5)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub1}", aff_sub1)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub2}", aff_sub2)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub3}", aff_sub3)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub4}", aff_sub4)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub5}", aff_sub5)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub6}", aff_sub6)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub7}", aff_sub7)
    ads_tracking_link = string.gsub(ads_tracking_link, "{aff_sub8}", aff_sub8)
    ads_tracking_link = string.gsub(ads_tracking_link, "%%%%", "%%")
    ngx.var.tracking_link = ads_tracking_link

    return ads_tracking_link
end


-- main start
ngx.var.es_time = 0
ngx.var.redis_time = 0
ngx.var.mysql_time = 0

local success, url = xpcall(process_http_request, function(e) ngx.log(ngx.ERR, debug.traceback()) return e end)
if success == false then
    ngx.header.content_type = 'text/plain'
    ngx.var.result_code = 1010
    ngx.log(ngx.ERR, "pcall error happened, request_uri: ", request_uri, ", err_info : ", url)
    ngx.say("please try again later.")
    ngx.exit(ngx.HTTP_OK)
end

if url == "-" or url == false then
    ngx.exit(ngx.HTTP_NO_CONTENT)
    return
end

if ngx.var.http_referer ~= nil then
    url = "http://www.cloudsmobi.com/forward.html?url=" .. ngx.escape_uri(url)
end
return ngx.redirect(url)

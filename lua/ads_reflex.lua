--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--

local cjson = require 'cjson.safe'
local http_helper = require "http_helper"
local redis_instance = require 'redis_helper'
local mysql_instance = require 'mysql_helper'
local util = require 'util'

--local request_uri = ngx.var.request_uri
local cjson_new = cjson.new()
local request_uri = ngx.var.request_uri

local LONG_EXPIRE_TIMEOUT = 60 * 60 * 24 * 90


-- remove trailing and leading whitespace from string.
-- http://en.wikipedia.org/wiki/Trim_(programming)
local function trim(string_need)
    -- from PiL2 20.4
    local encode = string.gsub(string_need, "%%", "%%%%")
    encode = (string.gsub(encode, "^%s*(.-)%s*$", "%1"))
    return encode
end

local function extract_number(table, item)
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

-- 检查是否需要扣量
--[[
    params:
    ded_rate    -- 扣量比例
    ded_count   -- 扣量的次数
    not_ded_count -- 没有扣量的次数

    result:
    bool -- true 触发扣量 false 无需扣量
--]]
local function is_need_deduction(ded_rate, ded_count, not_ded_count)
    local ded_block = 10 -- 扣量小区间N，用于分小段扣量
    local allow_request_count = 4 -- 默认允许请求量
    local current_pos = ded_count + not_ded_count + 1; -- 当前请求量
    local max_cap = math.ceil(current_pos / ded_block) * ded_block -- 当前第M区间末端值

    -- 判断是否已经基本请求量
    if (not_ded_count < allow_request_count) then
        return false
    end

    -- 判断是否已经扣量完成
    if (ded_count >= math.ceil(max_cap * ded_rate / 100)) then
        return false
    end

    -- 判断是否达到必须扣量的位置，若否，则随机即可
    if (current_pos < (max_cap - math.ceil(ded_block * ded_rate / 100))) then
        -- 如果是随机的，则提高扣量的几率
        local judge_rate = ded_rate
        if (ded_rate < 50) then
            judge_rate = 50
        end
        math.randomseed(os.time())
        return math.random(1, 100) <= judge_rate
    end

    return true
end

local function process_http_request()
    -- get unique_id
    local args = ngx.req.get_uri_args(10)
    local unique_id = args['click_id']
    local current_timestamp = ngx.now() -- seconds.milliseconds
    local dynamic_payout = extract_number(args['payout'], nil)
    if args['payout'] ~= nil and dynamic_payout <= 0 then
        ngx.log(ngx.ERR, "dynamic_payout error, unique_id : ", unique_id)
    end
    ngx.var.remote_address = util.through_proxy()
    local es_store_table = http_helper.http_get_es_affiliate_info(unique_id)
    if es_store_table == nil then
        ngx.var.error_msg = "get_affiliate_offer_info_by_unique_id error, id : " .. unique_id
        ngx.var.result_code = 2001
        return false
    end

    es_store_table = es_store_table["_source"]
    if es_store_table['ads_info'] ~= nil then
        ngx.var.result_code = 2007
        return true
    end

    -- 从redis里面读取unique_id，判断是否15秒内是否有重复记录,callback_search_[$unique_id]
    if not redis_instance.check_callback_exist("callback_search_" .. unique_id) then
        ngx.var.result_code = 2006
        return true
    end

    -- log affiliate info
    es_store_table["message"] = nil
    es_store_table["path"] = nil
    es_store_table["@version"] = nil
    ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
    ngx.var.invalidation = "true"
    -- 因为冒号:影响后面的拼接处理故在这将user_agent中的冒号统一转换成下划线_进行处理
    local http_user_agent = ngx.var.http_user_agent
    if http_user_agent == nil then
        http_user_agent = "-"
    end
    http_user_agent = string.gsub(http_user_agent, "\"", "_")
    ngx.var.local_user_agent = http_user_agent
    local campaign_id = es_store_table['campaign_id']
    local affiliate_id = es_store_table['affiliate_id']
    local smart_link_campaign_id = es_store_table['smart_campaign_id']
    if smart_link_campaign_id == nil then
        smart_link_campaign_id = campaign_id
    end

    -- get cache
    local ads_offer_info_table, user_offer_info_table = util.get_cache(affiliate_id, smart_link_campaign_id)
    if ads_offer_info_table == false then
        ngx.var.error_msg = "get_cache failed, write to log to process later."
        ngx.var.result_code = 2003
        return true
    end

    -- 广告主维度信息获取
    local ads_price = extract_number(ads_offer_info_table, "ads_price")
    local ads_start_time = extract_number(ads_offer_info_table, "start_time")
    local ads_end_time = extract_number(ads_offer_info_table, "end_time")
    local ads_daily_cap = extract_number(ads_offer_info_table, 'daily_cap')
    local ads_total_cap = extract_number(ads_offer_info_table, 'total_cap')

    -- 渠道维度信息获取
    local affiliate_price = extract_number(user_offer_info_table, "affiliate_price")
    local running_offer_id = extract_number(user_offer_info_table, "id")
    local affiliate_daily_cap = extract_number(user_offer_info_table, "daily_cap")
    local affiliate_total_cap = extract_number(user_offer_info_table, "total_cap")
    local affiliate_deduction_rate = extract_number(user_offer_info_table, "deduction_rate") -- 扣量几率

    -- redis data get
    local current_affiliate_daily_cap = extract_number(redis_instance.get("daily_cap-affiliate-running_offer-" .. running_offer_id), nil)
    local current_affiliate_total_cap = extract_number(redis_instance.get("total_cap-affiliate-running_offer-" .. running_offer_id), nil)
    local current_ads_daily_cap = extract_number(util.redis_get_shell(redis_instance.get, "daily_cap-ads-" .. smart_link_campaign_id), nil)
    local current_ads_total_cap = extract_number(util.redis_get_shell(redis_instance.get, "total_cap-ads—" .. smart_link_campaign_id), nil)


    -- dynamic price setting
    if dynamic_payout > 0 then
        -- 20170729 建旺：自动拉取的offer，当出现动态价格时，渠道回传价格为0.8倍，否则按照1.2倍30%扣量
        if ads_offer_info_table["auto_ads"] == 1 then
            affiliate_price = dynamic_payout * 0.8
            ads_offer_info_table["affiliate_price"] = affiliate_price
            user_offer_info_table["affiliate_price"] = affiliate_price
        end
        ads_price = dynamic_payout
        ads_offer_info_table["origin_ads_price"] = ads_offer_info_table["ads_price"]
        ads_offer_info_table["ads_price"] = ads_price
    end

    if ads_price < 0 or ads_price > 200 then
        ads_offer_info_table["origin_ads_price"] = ads_price
        ads_offer_info_table["ads_price"] = 0
        ads_price = 0
        es_store_table['decide_to_postback'] = "false"
        ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
        ngx.var.result_code = 2023
        ads_offer_info_table["affiliate_price"] = 0
        user_offer_info_table["affiliate_price"] = 0
        ngx.var.ads_offer_info = cjson_new.encode(ads_offer_info_table)
        ngx.var.user_offer_info = cjson_new.encode(user_offer_info_table)
        return true
    end

    -- 批量处理 todo::redis的操作目前还是多
    local redis_connection = redis_instance.get_connection()
    if redis_connection == false or redis_connection == nil then
        ngx.log(ngx.ERR, "redis connected failed")
    else
        redis_connection:init_pipeline()
        -- ads cap 计入redis缓存中
        redis_connection:incr("daily_cap-ads-" .. smart_link_campaign_id)
        redis_connection:expire("daily_cap-ads-" .. smart_link_campaign_id, LONG_EXPIRE_TIMEOUT)
        redis_connection:incr("total_cap-ads-" .. smart_link_campaign_id)
        redis_connection:expire("total_cap-ads-" .. smart_link_campaign_id, LONG_EXPIRE_TIMEOUT)
        -- 广告主方向的回传计入收入
        redis_connection:incr("statics_ads_cv-" .. ads_offer_info_table['adviser_id'] .. "-" .. smart_link_campaign_id)
        redis_connection:incrbyfloat("statics_ads_revenue-" .. ads_offer_info_table['adviser_id'] .. "-" .. smart_link_campaign_id, ads_price)
        -- 该渠道创造的收入
        redis_connection:incrbyfloat("statics_affiliate_ads_revenue-" .. affiliate_id .. "-" .. campaign_id, ads_price)
        local results, err = redis_connection:commit_pipeline()
        if not results then
            ngx.log(ngx.ERR, "redis failed to commit the pipelined requests: ", err)
        end

        redis_connection:set_keepalive(1000, 1000)
    end

    -- 动态价格出现问题，广告主并未回传价格时，该回传不发送回渠道，也不计入渠道收益中。
    if affiliate_price < 0 then
        if ads_offer_info_table['conversion_flow'] == "CPD" or
                ads_offer_info_table['adviser_id'] == "255" or ads_offer_info_table['adviser_id'] == "300" or
                ads_offer_info_table['adviser_id'] == "311" or ads_offer_info_table['adviser_id'] == "303" then
            ads_offer_info_table["affiliate_price"] = ads_price
            user_offer_info_table["affiliate_price"] = ads_price
            affiliate_price = ads_price
        end
        if affiliate_price < 0 then
            es_store_table['decide_to_postback'] = "false"
            ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
            ngx.var.result_code = 2013
            ads_offer_info_table["affiliate_price"] = affiliate_price
            ads_offer_info_table["ads_price"] = ads_price
            user_offer_info_table["affiliate_price"] = affiliate_price
            ngx.var.ads_offer_info = cjson_new.encode(ads_offer_info_table)
            ngx.var.user_offer_info = cjson_new.encode(user_offer_info_table)
            return true
        end
    end

    ngx.var.ads_offer_info = cjson_new.encode(ads_offer_info_table)
    ngx.var.user_offer_info = cjson_new.encode(user_offer_info_table)

    if es_store_table['decide_to_postback'] ~= "true" then
        ngx.var.result_code = 2011
        return true
    end

    if user_offer_info_table["decide_to_postback"] == 0 or user_offer_info_table["status"] ~= 2 then
        es_store_table['decide_to_postback'] = "false"
        ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
        ngx.var.result_code = 2010
        return true
    end

    -- 判断ads使用量
    if (ads_total_cap ~= 0 and current_ads_total_cap >= ads_total_cap) or
            (ads_daily_cap ~= 0 and current_ads_daily_cap >= ads_daily_cap) then
        es_store_table['decide_to_postback'] = "false"
        ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
        ngx.var.result_code = 2014
        return true
    end

    if current_timestamp <= ads_start_time or current_timestamp >= ads_end_time then
        es_store_table['decide_to_postback'] = "false"
        ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
        ngx.var.result_code = 2010
        return true
    end

    -- 累计cap,根据cap,time 等判断是否需要进行回传
    -- current_timestamp <= affiliate_start_time or current_timestamp >= affiliate_end_time then 暂时affiliate 维度的时间未投入使用
    if (affiliate_total_cap ~= 0 and current_affiliate_total_cap >= affiliate_total_cap) or
            (affiliate_daily_cap ~= 0 and current_affiliate_daily_cap >= affiliate_daily_cap) then
        es_store_table['decide_to_postback'] = "false"
        ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
        ngx.var.result_code = 2009
        return true
    end

    -- 扣量v2
    if ngx.var.deduction_status == '1' then
        local deduction_ded_count_cache_key = 'deduction_ded_count-affiliate-running_offer-' .. running_offer_id -- 已扣量key
        local decuction_not_ded_count_cache_key = 'deduction_not_ded_count-affiliate-running_offer-' .. running_offer_id -- 未扣量key
        local ded_count = extract_number(redis_instance.get(deduction_ded_count_cache_key), nil)
        local not_ded_count = extract_number(redis_instance.get(decuction_not_ded_count_cache_key), nil)

        -- 扣量检测
        local deduction_result = is_need_deduction(affiliate_deduction_rate, ded_count, not_ded_count);
        -- 触发扣量
        if (deduction_result) then
            if not redis_instance.incr(deduction_ded_count_cache_key) then
                ngx.log(ngx.ERR, "redis increase " .. deduction_ded_count_cache_key .. " fail.")
            end
            es_store_table['decide_to_postback'] = "false"
            ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
            ngx.var.result_code = 2012
            return true
        end
        -- 不扣量
        if not redis_instance.incr(decuction_not_ded_count_cache_key) then
            ngx.log(ngx.ERR, "redis increase " .. decuction_not_ded_count_cache_key .. " fail.")
        end
    end

    -- 爆量
    local affiliate_callback_rule_cache_key = 'affiliate_callback_rule-affiliate-id-' .. affiliate_id .. '-offer-id-' .. campaign_id
    local affiliate_callback_rule_cache_value = redis_instance.get(affiliate_callback_rule_cache_key)
    if affiliate_callback_rule_cache_value ~= nil and affiliate_callback_rule_cache_value ~= false then
        affiliate_callback_rule_cache_value = cjson_new.decode(affiliate_callback_rule_cache_value)
        if affiliate_callback_rule_cache_value["over_quantity"] == true then
            ngx.log(ngx.NOTICE, "over quantity." .. 'affiliate id:' .. affiliate_id .. ' , offer id:' .. campaign_id)
            ngx.var.error_msg = "error: over quantity"
            es_store_table['decide_to_postback'] = "false"
            ngx.var.affiliate_info = string.sub(cjson_new.encode(es_store_table), 2, -2)
            ngx.var.result_code = 2015
            return true
        end
    end

    -- 批量处理
    redis_connection = redis_instance.get_connection()
    if redis_connection == false or redis_connection == nil then
        ngx.log(ngx.ERR, "redis connected failed")
    else
        redis_connection:init_pipeline()
        -- affiliate cap 计入redis缓存中
        redis_connection:incr("daily_cap-affiliate-running_offer-" .. running_offer_id)
        redis_connection:expire("daily_cap-affiliate-running_offer-" .. running_offer_id, LONG_EXPIRE_TIMEOUT)
        redis_connection:incr("total_cap-affiliate-running_offer-" .. running_offer_id)
        redis_connection:expire("total_cap-affiliate-running_offer-" .. running_offer_id, LONG_EXPIRE_TIMEOUT)
        -- 渠道方向的回传计入收入
        redis_connection:incr("statics_affiliate_cv-" .. affiliate_id .. "-" .. campaign_id)
        redis_connection:incrbyfloat("statics_affiliate_revenue-" .. affiliate_id .. "-" .. campaign_id, affiliate_price)
        -- 广告主维度的支出计入
        redis_connection:incrbyfloat("statics_ads_expense-" .. ads_offer_info_table['adviser_id'] .. "-" .. smart_link_campaign_id, affiliate_price)
        local results, err = redis_connection:commit_pipeline()
        if not results then
            ngx.log(ngx.ERR, "redis failed to commit the pipelined requests: ", err)
        end

        redis_connection:set_keepalive(1000, 1000)
    end

    local postback_url = user_offer_info_table['postback_url']
    if postback_url == nil or postback_url == "" or string.upper(string.sub(postback_url, 0, 4)) ~= "HTTP" then
        postback_url = mysql_instance.get_user_default_postback_url(affiliate_id)
    end
    if es_store_table['request_params']['aff_sub1'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub1}", trim(es_store_table['request_params']['aff_sub1']))
    end
    if es_store_table['request_params']['aff_sub2'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub2}", trim(es_store_table['request_params']['aff_sub2']))
    end
    if es_store_table['request_params']['aff_sub3'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub3}", trim(es_store_table['request_params']['aff_sub3']))
    end
    if es_store_table['request_params']['aff_sub4'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub4}", trim(es_store_table['request_params']['aff_sub4']))
    end
    if es_store_table['request_params']['aff_sub5'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub5}", trim(es_store_table['request_params']['aff_sub5']))
    end
    if es_store_table['request_params']['aff_sub6'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub6}", trim(es_store_table['request_params']['aff_sub6']))
    end
    if es_store_table['request_params']['aff_sub7'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub7}", trim(es_store_table['request_params']['aff_sub7']))
    end
    if es_store_table['request_params']['aff_sub8'] ~= nil then
        postback_url = string.gsub(postback_url, "{aff_sub8}", trim(es_store_table['request_params']['aff_sub8']))
    end
    if es_store_table['request_params']['subpub'] ~= nil then
        postback_url = string.gsub(postback_url, "{subpub}", trim(es_store_table['request_params']['subpub']))
    end

    -- 额外的参数 {offer_id}, {offer_name}, {aff_id}/{affiliate_id}, {click_ip}, {datetime}, {click_id}/{transaction_id}, {payout}
    postback_url = string.gsub(postback_url, "{offer_id}", campaign_id)
    postback_url = string.gsub(postback_url, "{offer_name}", extract_number(ads_offer_info_table, "campaign_name"))
    postback_url = string.gsub(postback_url, "{aff_id}", affiliate_id)
    postback_url = string.gsub(postback_url, "{affiliate_id}", affiliate_id)
    if es_store_table['remote_addr'] ~= nil then
        postback_url = string.gsub(postback_url, "{click_ip}", es_store_table['remote_addr'])
    end
    postback_url = string.gsub(postback_url, "{datetime}", os.date("!%Y-%m-%d %d-%M-%S"))
    postback_url = string.gsub(postback_url, "{click_id}", unique_id)
    postback_url = string.gsub(postback_url, "{transaction_id}", unique_id)
    postback_url = string.gsub(postback_url, "{payout}", affiliate_price)
    postback_url = trim(postback_url)
    postback_url = string.gsub(postback_url, "%%%%", "%%")
    user_offer_info_table['postback_url'] = postback_url

    -- todo :: json转化次数太多,优化
    ngx.var.user_offer_info = cjson_new.encode(user_offer_info_table)

    -- visit affiliate postback url
    local result, msg = pcall(http_helper.http_send_request, postback_url)
    if result == false then
        ngx.var.result_code = 2008
        return true
    elseif msg == false then
        ngx.var.error_msg = "need to try again, url:" .. postback_url
        ngx.var.result_code = 2005
    end
    return true
end


-- main start
ngx.var.es_time = 0
ngx.var.redis_time = 0
ngx.var.mysql_time = 0

local success, result = xpcall(process_http_request, function(e) ngx.log(ngx.ERR, debug.traceback()) return e end)
if success == false then
    ngx.var.result_code = 2100
    ngx.log(ngx.ERR, "pcall error happened, request_uri: ", request_uri, ", err_info : ", result)
    --    ngx.header.content_type = 'text/plain'
    --    ngx.say("failed")
    ngx.exit(ngx.HTTP_BAD_REQUEST)
elseif result == false then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

ngx.header.content_type = 'text/plain'
ngx.say("success")
ngx.exit(ngx.HTTP_OK)

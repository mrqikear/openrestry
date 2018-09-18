--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--

-- cache strategy table per worker
local cjson = require 'cjson.safe'
local _M = {}
local st = {}

function _M.get_strategy(domain)
    local ver = domain .."version"
    local config = ngx.shared.redirect_config

    if st[domain] and st[domain]["version"] and
        st[domain]["version"]["sn"] == config:get(ver) then
        return st[domain]
    end

    local jsonstr = config:get(domain)
    if not jsonstr then
        return nil, domain .." no strategy available yet!"
    end

    local cjson_new = cjson.new()
    local strategy_dict, err = cjson_new.decode(jsonstr)
    if not strategy_dict then
        return nil, domain .." strategy cache decode failed: "..err
    end

    st[domain] = strategy_dict
    ngx.log(ngx.WARN, "strategy cache updated for ", domain,
            ", ver ", strategy_dict["version"]["sn"])

    return st[domain]
end

return _M

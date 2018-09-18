--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 16/5/13
-- Time: 10:54
--

local iplib = require 'iplib'

-- local config_path = "/Users/wyne/webroot/ys_web/reflex_chain/lua/conf/reflex_conf.json"
-- local acl_config_path = "/Users/wyne/webroot/ys_web/reflex_chain/lua/conf/acl-iplib.txt"
-- online
local config_path = "/home/work/workplace/ys_web/reflex_chain/lua/conf/reflex_conf_online.json"
-- offline
--local config_path = "/home/work/ys_web/reflex_chain/lua/conf/reflex_conf.json"
local acl_config_path = "/home/work/workplace/ys_web/reflex_chain/lua/conf/acl-iplib.txt"

-- load acl-iplib
iplib.init(acl_config_path)

-- read conf and cache
local conf_file, errmsg = io.open(config_path, "r")
if not conf_file then
    error(errmsg)
end
local content = conf_file:read("*a")
conf_file:close()

local lua_config = ngx.shared.shared_lua_conf_dict
local success = lua_config:set('lua_config', content)
if not success then
    error("set lua config failed..")
    return
end

-- ngx.log(ngx.ALERT, "local idc name ", idc_name)

--local success = config:set('nginx_services_table', "")
--if not success then
--    error("set nginx_services_table failed..")
--    return
--end

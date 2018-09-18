--
-- Created by IntelliJ IDEA.
-- User: wyne(wyne.lu@gmail.com)
-- Date: 2015/7/5
-- Time: 10:54
--

local _M = {}
local iplib = {}

local function bton(binary_addr)
    local s1, s2, s3, s4 = string.byte(binary_addr, 1, 4)
    return s1 * 16777216 + s2 * 65536 + s3 * 256 + s4
end

local function inet_pton(s)
    local _, _, s1, s2, s3, s4 = string.find(s, '^(%d+)%.(%d+)%.(%d+)%.(%d+)$')
    if (not s1) or (not s2) or (not s3) or (not s4) then
        return nil, ': ' .. s .. ' is not a valid ipv4 ip'
    end

    -- no number range checking here (0~255)
    return tonumber(s1) * 16777216 + tonumber(s2) * 65536 + tonumber(s3) * 256 + tonumber(s4)
end

local function bsearch(a, v)
    local low = 1
    local high = #a

    while (low <= high) do
        local mid = math.floor((low + high) / 2)
        if a[mid]["eip"] < v then
            low = mid + 1
        elseif a[mid]["sip"] > v then
            high = mid - 1
        elseif a[mid]["sip"] <= v and v <= a[mid]["eip"] then
            return mid
        else
            return nil -- should not reach here
        end
    end
    return nil
end

--@ip: binary ipv4 network-ordered string, e.g $binary_remote_addr
function _M.locate(ip)
    local i = bsearch(iplib, inet_pton(ip))
    if i then
        return iplib[i]
    else
        return nil
    end
end

function _M.init(file)
    local ipf, errmsg = io.open(file, "r")
    if not ipf then
        error(errmsg)
    end

    local i = 1
    local maxip = 0
    while true do
        local line = ipf:read("*line")
        if line == nil then break end

        local _, _, sip, eip, area =
            string.find(line, '^(%d+%.%d+%.%d+%.%d+)|(%d+%.%d+%.%d+%.%d+)|(%a+)')
        if (not sip) or (not eip) or (not area) then
            error("ip lib line " .. tostring(i) .. " format error")
        end

        sip, errmsg = inet_pton(sip)
        if not sip then
            error("ip lib line " .. tostring(i) .. errmsg)
        end

        eip, errmsg = inet_pton(eip)
        if not eip then
            error("ip lib line " .. tostring(i) .. errmsg)
        end

        if sip > eip then
            error("ip lib line " .. tostring(i) .. " sip > eip")
        end
        if sip < maxip then
            error("ip lib line " .. tostring(i) .. " ascended order required")
        end

        maxip = eip
        iplib[i] = { ["sip"] = sip, ["eip"] = eip, ["acl"] = area}
        i = i + 1
    end

    ipf:close()
end

return _M

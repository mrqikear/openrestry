#!/bin/sh

rm -rf addon/anti-attack-module/src/policyframe/
cd addon/anti-attack-module/src/
tar -zxf policyframe.tar.gz 
cd -

cd addon/
rm -rf lua
tar -zxf lua.tgz 
cd -

cd addon/bmat
rm -f libbmatcdn.a
tar zxf libmat.tar.gz
cd -

cd addon/
tar zxf pcre.tar.gz
cd -

#baidu's gcc version too low, not support luajit
export LUAJIT_LIB=addon/luajit/lib
export LUAJIT_INC=addon/luajit/include/luajit-2.0
#export LUA_LIB=addon/lua/lib
#export LUA_INC=addon/lua/include

./configure \
	--with-debug \
	--with-http_realip_module \
	--add-module=addon/jstart \
	--add-module=addon/upstream_jhash \
	--add-module=addon/upstream_jdomain \
	--add-module=addon/jflow_control \
	--add-module=addon/jbilling \
	--add-module=addon/headers_more \
	--add-module=addon/blowfish \
	--add-module=addon/bmat \
	--add-module=addon/md5check \
	--add-module=addon/echo-nginx-module-master \
	--with-http_stub_status_module \
	--with-pcre=addon/pcre-8.01/ \
	--with-ld-opt="-lstdc++" --add-module=addon/anti-attack-module \
	--with-http_gunzip_module \
	--add-module=addon/lua-nginx-module-0.9.5rc2 \
	--add-module=addon/ngx_devel_kit-0.2.19 \
	--add-module=addon/lua-dynamic-upstream \
	--prefix=output

#make -j 8
#make install

#mkdir -p output/lua && cp -r lua/* output/lua/
#mkdir -p output/conf && cp -r conf/* output/conf/
#mkdir -p output/policy_conf && cp -r policy_conf/* output/policy_conf/

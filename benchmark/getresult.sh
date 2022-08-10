cd ../logs/

rm logs/*.json

scp mpc-0:/home/zhan/narwhal/logs/*.json ./
scp mpc-1:/home/zhan/narwhal/logs/*.json ./
scp mpc-2:/home/zhan/narwhal/logs/*.json ./
scp mpc-3:/home/zhan/narwhal/logs/*.json ./
scp mpc-4:/home/zhan/narwhal/logs/*.json ./
scp mpc-5:/home/zhan/narwhal/logs/*.json ./
scp mpc-6:/home/zhan/narwhal/logs/*.json ./
scp mpc-7:/home/zhan/narwhal/logs/*.json ./
scp mpc-8:/home/zhan/narwhal/logs/*.json ./
scp mpc-9:/home/zhan/narwhal/logs/*.json ./

cd ../mpc/
fab summary


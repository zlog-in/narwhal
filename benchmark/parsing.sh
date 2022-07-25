cd /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/
rm logs/*.log
rm logs/*.json



#for i in {0..9}
#do
#    scp mpc-$i:/home/zhan/narwhal/logs/client-$i-0.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
#    scp mpc-$i:/home/zhan/narwhal/logs/worker-$i-0.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
#    scp mpc-$i:/home/zhan/narwhal/logs/primary-$i.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
 #   for j in {1..9}
  #  do
   #     scp mpc-$i:/home/zhan/narwhal/logs/client-$j$i-0.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
    #    scp mpc-$i:/home/zhan/narwhal/logs/worker-$j$i-0.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
     #   scp mpc-$i:/home/zhan/narwhal/logs/primary-$j$i.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
    #done
#done

scp mpc-0:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-0:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-1:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-1:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-2:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-2:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-3:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-3:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-4:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-4:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-5:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-5:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-6:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-6:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-7:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-7:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-8:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-8:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/

scp mpc-9:/home/zhan/narwhal/logs/*.log /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/
scp mpc-9:/home/zhan/narwhal/logs/*.json /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/logs/


cd /home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/
fab logs 


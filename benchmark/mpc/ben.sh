source /root/.cargo/env
cd /home/narwhal/benchmark/
git checkout benchmarking
git restore .committee.json
git pull
#rm logs/*.log

fab local

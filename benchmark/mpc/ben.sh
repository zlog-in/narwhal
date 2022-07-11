source /root/.cargo/env
cd /home/hotstuff/benchmark/
git checkout benchmarking
git restore .committee.json
git pull
#rm logs/*.log

fab local

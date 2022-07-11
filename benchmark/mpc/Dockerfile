FROM ubuntu:latest

RUN apt update && apt install git vim clang tmux curl python3-pip fabric -y  && curl https://sh.rustup.rs -sSf | bash -s -- -y && echo 'source $HOME/.cargo/env' >> $HOME/.bashrc && cd /home/ && git clone https://github.com/zlog-in/narwhal/ && cd /home/narwhal/benchmark/ && git checkout benchmarking && pip install -r requirements.txt

WORKDIR /home/narwhal/benchmark

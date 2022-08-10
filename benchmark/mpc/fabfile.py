from sqlite3 import Time
from statistics import mean
from fabric import Connection, ThreadingGroup
from fabric import task
import subprocess
import random
import json
from datetime import datetime
import sqlite3

import os

@task
def benchmarking(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/config.json', remote  = '/home/zhan/narwhal/')
    hosts.run('docker stop hotstuff')
    hosts.run('docker start narwhal')
    hosts.run('docker cp narwhal/config.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker exec -t narwhal bash ben.sh')

@task
def faulty(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    faulty_config()
    hosts.put(f'{os.pardir}/faulty.json', remote  = '/home/zhan/narwhal/')
    hosts.put(f'{os.pardir}/bench_parameters.json', remote  = '/home/zhan/narwhal/')
    hosts.put(f'{os.pardir}/node_parameters.json', remote  = '/home/zhan/narwhal/')
    # hosts.run('docker stop narwhal')
    hosts.run('docker stop hotstuff')
    hosts.run('docker start narwhal')
    # hosts.run('docker cp narwhal/config.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/faulty.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/bench_parameters.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/node_parameters.json narwhal:/home/narwhal/benchmark/')
    hosts.run('docker exec -t narwhal bash ben.sh')






@task
def container(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.run('rm -rf narwhal/logs/')
    hosts.run('mkdir -p narwhal/logs')
    hosts.run('docker stop hotstuff')
    hosts.put(f'{os.getcwd()}/ben.sh', remote='/home/zhan/narwhal')
    hosts.put(f'{os.getcwd()}/update.sh', remote='/home/zhan/narwhal')

    hosts.run('docker rm -f narwhal')
    hosts.run('docker run -itd --name narwhal -p 9000-9049:9000-9049 --mount type=bind,source=/home/zhan/narwhal/logs,destination=/home/narwhal/benchmark/logs image_narwhal')
    hosts.run('docker cp index.txt narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/ben.sh narwhal:/home/narwhal/benchmark/')
    hosts.run('docker cp narwhal/update.sh narwhal:/home/narwhal/benchmark/')
    hosts.run('docker exec -t narwhal bash update.sh')



@task
def parsing(ctx):
    subprocess.call(['bash', '../parsing.sh'])

@task
def getresult(ctx):
    subprocess.call(['bash', '../getresult.sh'])

@task
def summary(ctx):
    with open('../bench_parameters.json') as f:
        bench_parameters = json.load(f)
        f.close()
    consensus_bytes_list = []
    consensus_start_list = []
    consensus_end_list = []
    consensus_latency_list = []
    consensus_size_list = []
    consensus_bps_list = []
    consensus_tps_list = []
    end2end_bytes_list = []
    end2end_start_list = []
    end2end_end_list = []
    end2end_latency_list = []
    end2end_size_list = []
    end2end_bps_list = []
    end2end_tps_list = []

    for node_i in range(bench_parameters['servers']):
        with open(f'../logs/result-{node_i}.json') as f:
            result = json.load(f)
            f.close()
            consensus_bytes_list.append(result['consensus_bytes'])
            consensus_start_list.append(result['consensus_start'])
            consensus_end_list.append(result['consensus_end'])
            consensus_latency_list.append(result['consensus_latency'])
            consensus_size_list.append(result['consensus_size'])
            consensus_bps_list.append(result['consensus_bps'])
            consensus_tps_list.append(result['consensus_tps'])
            end2end_bytes_list.append(result['end2end_bytes'])
            end2end_start_list.append(result['end2end_start'])
            end2end_end_list.append(result['end2end_end'])
            end2end_latency_list.append(result['end2end_latency'])
            end2end_size_list.append(result['end2end_size'])
            end2end_bps_list.append(result['end2end_bps'])
            end2end_tps_list.append(result['end2end_tps'])
    
    # print(consensus_bytes_list)
    # print(end2end_bytes_list)

    consensus_duration = max(consensus_end_list) - min(consensus_start_list)
    end2end_duration = max(end2end_end_list) - min(end2end_start_list)
    # print(consensus_duration)
    # print(end2end_duration)
    
    # consensus_bps = (sum(consensus_bytes_list)) / consensus_duration
    # end2end_bps = (sum(end2end_bytes_list)) / end2end_duration
    # consensus_tps = consensus_bps / result['consensus_size']
    # end2end_tps = end2end_bps / result['end2end_size']
    # print(consensus_bps_list)
    # print(consensus_tps_list)
    consensus_bps = sum(consensus_bps_list)
    consensus_tps = sum(consensus_tps_list)
    end2end_bps = sum(end2end_bps_list)
    end2end_tps = sum(end2end_tps_list)
    # print(round(consensus_bps), round(consensus_tps))
    # print(round(end2end_bps), round(end2end_tps))

    consensus_latency = mean(consensus_latency_list)
    end2end_latency = mean(end2end_latency_list)
    # print(round(consensus_latency), round(end2end_latency))



    replicas = bench_parameters['replicas']
    servers = bench_parameters['servers']
    local = bench_parameters['local'] 
    duration = bench_parameters['duration']  
    rate = bench_parameters['rate'] 
    faults = bench_parameters['faults']
    delay = bench_parameters['delay']
    nodes = replicas * servers
    
    with open('../faulty.json') as f:
            faulty_config = json.load(f)
            f.close()
    time_seed = faulty_config['time_seed']
    results_db = sqlite3.connect('./results.db')
    if faults == 0 and delay == 0:
        insert_S1Narwhal_results = f'INSERT INTO S1Narwhal VALUES ("{time_seed}", {local}, {nodes}, {faults}, {duration}, {rate}, {round(consensus_tps)}, {round(consensus_latency)}, {round(end2end_latency)})'
        results_db.cursor().execute(insert_S1Narwhal_results)
        results_db.commit()
        results_db.close()
    
    elif faults > 0 and delay ==0:
        insert_S2Narwhal_results = f'INSERT INTO S2Narwhal VALUES ("{time_seed}", {local}, {nodes}, {faults}, {duration}, {rate}, {round(consensus_tps)}, {round(consensus_latency)}, {round(end2end_latency)})'
        results_db.cursor().execute(insert_S2Narwhal_results)
        results_db.commit()
        results_db.close()
    
    elif delay > 0 and faults == 0:
        print("S3")

@task
def getdb(ctx):
    host = Connection('checker')
    host.get('/home/zhan/narwhal/benchmark/mpc/results.db', local = f'{os.getcwd()}/results/')
        
@task
def checker(ctx):
    host = Connection('checker')
    host.put('./checker.py', remote='narwhal/benchmark/mpc/')
    host.put('../faulty.json', remote = 'narwhal/benchmark/')
    host.put('../bench_parameters.json', remote = 'narwhal/benchmark/')
    host.put('../node_parameters.json', remote = 'narwhal/benchmark/')

@task
def build(ctx):
    hosts = ThreadingGroup('mpc-0','mpc-1','mpc-2','mpc-3','mpc-4','mpc-5','mpc-6','mpc-7','mpc-8','mpc-9')
    hosts.run('rm -rf narwhal/')
    hosts.run('mkdir  narwhal/')
    #hosts.run('docker stop hotstuff')
    hosts.put('/home/z/Sync/Study/DSN/Marc/Code/narwhal/benchmark/mpc/Dockerfile', remote='/home/zhan/narwhal')
    hosts.run('docker rm -f narwhal')
    hosts.run('docker rmi image_narwhal')
    hosts.run('docker build -f /home/zhan/narwhal/Dockerfile -t image_narwhal .')


def faulty_config():
    with open('../bench_parameters.json', 'r') as f:
        bench_parameters = json.load(f)
        f.close()
    faults = bench_parameters['faults']
    servers = bench_parameters['servers']
    duration = bench_parameters['duration']
    replicas = bench_parameters['replicas']
    faulty_servers = set()
    #time_seed = read_time()
    time_seed = datetime.now()
    random.seed(time_seed)
    while len(faulty_servers) != faults:
        faulty_servers.add(random.randrange(0, servers*replicas))
    print(faulty_servers)
    
    with open('../faulty.json', 'w') as f:
        print("The json for faulty servers is created")
        json.dump({f'{idx}': [0,0] for idx in range(servers * replicas)}, f, indent=4)
        f.close()

    
    with open('../faulty.json', 'r') as f:
        faulty_config = json.load(f)
        f.close()
    # faulty_config['0'][1] = faults

    
    while len(faulty_servers) != 0:
        idx = faulty_servers.pop()
        faulty_config[f'{idx}'][0] = 1
        faulty_config[f'{idx}'][1] = random.randrange(10,duration)
    
    with open('../faulty.json', 'w') as f:
        json.dump(faulty_config, f, indent=4)
        f.close()

    write_time(time_seed)

def write_time(seed):
    with open(f'../faulty.json') as f:
        faulty_config = json.load(f)
        f.close()
    faulty_config.update({'time_seed': f'{seed}'})

    with open('../faulty.json', 'w') as f:
        json.dump(faulty_config, f, indent=4)
        f.close()

def read_time():
    with open(f'../faulty.json') as f:
        faulty_config = json.load(f)
        f.close()
    return faulty_config['time_seed']



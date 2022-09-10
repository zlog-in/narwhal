# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil, floor
from os.path import basename, splitext
from time import sleep
import json
from threading import Thread

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError

from benchmark.utils import Print, BenchError, PathMaker

import os


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            # print(self.bench_parameters)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        #print(name)
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)
    
    def _kill_faulty(self, id, duration):
        print(f'replica {id} is faulty and will be crashed after {duration}s execution')
        sleep(duration)
        
        subprocess.run(['tmux', 'kill-session', '-t', f'primary-{id}'])
        subprocess.run(['tmux', 'kill-session', '-t', f'worker-{id}-0'])
        subprocess.run(['tmux', 'kill-session', '-t', f'client-{id}-0'])
        print(f'and replica {id} crashed after {duration}s exectution')

    def _delay(self, node_i, delay, delay_duration):
        sleep(5) # after 5s of consensus process
        print(f'Communication delay for server {node_i} increases to {delay}ms for duration {delay_duration}s')
        # subprocess.run(f'tc qdisc add dev eth0 root netem delay {delay}ms {round(delay/10)}ms distribution normal', shell = True)# specification about delay distribution 
        subprocess.run(f'tc qdisc add dev eth0 root netem delay {delay}ms', shell = True)# specification about delay distribution 
        sleep(delay_duration)
        subprocess.run('tc qdisc del dev eth0 root', shell=True)
        print(f'Communication delay for server {node_i} ends after {delay_duration}s')

    def _partition(self, targets, start, end):
        print(f'{start}s normal network before partition')
        sleep(start)
        print(f'Partition into two networks happened for {end}s ')
        
        # os.popen('tc qdisc del dev eth0 root')
        os.popen('tc qdisc add dev eth0 root handle 1: prio')
        os.popen('tc qdisc add dev eth0 parent 1:3 handle 30: netem loss 100%')
        for tar in targets:
            os.popen(f'tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst {tar} flowid 1:3')
        sleep(end)
        os.popen('tc qdisc del dev eth0 root')

        
    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            Print.info('Reading configuration')

            
            nodes, rate, replicas, servers, local, faults, S2f, duration, delay, partition = self.nodes[0], self.rate[0], self.replicas, self.servers, self.local, self.faults, self.S2f, self.duration, self.delay, self.partition

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            print("For the first run, it takes a little longer to compile")
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                # if local == True:
                #     subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
            with open('index.txt') as f:
                node_i = int(f.readline())
                f.close()

            node_ip = '127.0.0.1'

            if node_i == 0:
                node_ip = '129.13.88.182'
            elif node_i == 1:
                node_ip = '129.13.88.183'
            elif node_i == 2 :
                node_ip = '129.13.88.184'
            elif node_i == 3:
                node_ip = '129.13.88.185'
            elif node_i == 4:
                node_ip = '129.13.88.186'
            elif node_i == 5:
                node_ip = '129.13.88.187'
            elif node_i == 6:
                node_ip = '129.13.88.188'
            elif node_i == 7:
                node_ip = '129.13.88.189'
            elif node_i == 8:
                node_ip = '129.13.88.190'
            elif node_i == 9:
                node_ip = '129.13.88.180'
            
            ips = ['129.13.88.182', '129.13.88.183', '129.13.88.184', '129.13.88.185', '129.13.88.186', '129.13.88.187', '129.13.88.188', '129.13.88.189', '129.13.88.190', '129.13.88.180']

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers, local, servers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())
            # Run the clients (they will wait for the nodes to be ready).
            # workers_addresses = committee.workers_addresses(self.faults)
            workers_addresses = committee.workers_addresses(0)
            rate_share = ceil(rate / committee.workers())   # ceil or floor
            if local == False:
                for i, addresses in enumerate(workers_addresses):
                    for (id, address) in addresses:
                        addr_ip = address[:-5]
                        if node_ip == addr_ip:
                            #print("!!!!!!!!!!!!!!!")
                            cmd = CommandMaker.run_client(
                                address,
                                self.tx_size,
                                rate_share,
                                [x for y in workers_addresses for _, x in y] 
                            )
                            #print(f"cmd for client on node {node_ip}")
                            #print(cmd)
                            log_file = PathMaker.client_log_file(i, id)
                            self._background_run(cmd, log_file)
            if local == True:
                for i, addresses in enumerate(workers_addresses):
                    for (id, address) in addresses:
                        cmd = CommandMaker.run_client(
                            address,
                            self.tx_size,
                            rate_share,
                            [x for y in workers_addresses for _, x in y]
                        )
                        log_file = PathMaker.client_log_file(i, id)
                        self._background_run(cmd, log_file)

            # Run the primaries (except the faulty ones).
            if local == False:
                for i, address in enumerate(committee.primary_addresses(0)):
                    if node_i == i % servers:
                        cmd = CommandMaker.run_primary(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            PathMaker.db_path(i),
                            PathMaker.parameters_file(),
                            debug=debug
                        )
                        # print("cmd for primaries")
                        # print(cmd)
                        log_file = PathMaker.primary_log_file(i)
                        self._background_run(cmd, log_file)
            if local == True:
                for i, address in enumerate(committee.primary_addresses(0)):
                    cmd = CommandMaker.run_primary(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i),
                        PathMaker.parameters_file(),
                        debug=debug
                    )
                    log_file = PathMaker.primary_log_file(i)
                    self._background_run(cmd, log_file)
            

            # Run the workers (except the faulty ones).
            if local == False:
                for i, addresses in enumerate(workers_addresses):
                    if node_i == i % servers:
                        for (id, address) in addresses:
                            cmd = CommandMaker.run_worker(
                                PathMaker.key_file(i),
                                PathMaker.committee_file(),
                                PathMaker.db_path(i, id),
                                PathMaker.parameters_file(),
                                id,  # The worker's id.
                                debug=debug                       
                            )
                            # print("cmd for works")
                            # print(cmd)
                            log_file = PathMaker.worker_log_file(i, id)
                            self._background_run(cmd, log_file)
            if local == True:
                for i, addresses in enumerate(workers_addresses):
                    for (id, address) in addresses:
                        cmd = CommandMaker.run_worker(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            PathMaker.db_path(i, id),
                            PathMaker.parameters_file(),
                            id,  # The worker's id.
                            debug=debug
                        )
                        log_file = PathMaker.worker_log_file(i, id)
                        self._background_run(cmd, log_file)


            
            Print.info(f'Running benchmark ({duration} sec)...')
            if faults > 0 and delay == 0 and S2f == False:
                with open('faulty.json','r') as f:
                    faulty_config = json.load(f)
                    f.close()
                for r in range(replicas):
                    # print(f'r: {r}')
                    replica_i = node_i + r * servers
                    flag = faulty_config[f'{replica_i}'][0]
                    if flag == 1:
                        # print(f'flag: {flag}')
                        faulty_duration = faulty_config[f'{replica_i}'][1]
                        Thread(target=self._kill_faulty, args=(replica_i,faulty_duration)).start()
            
            elif faults >= 0 and delay == 0 and S2f == True:
                
                if faults > 0:
                    with open('faulty.json','r') as f:
                        faulty_config = json.load(f)
                        f.close()
                    for r in range(replicas):
                        # print(f'r: {r}')
                        replica_i = node_i + r * servers
                        flag = faulty_config[f'{replica_i}'][0]
                        if flag == 1:
                            # print(f'flag: {flag}')
                            faulty_duration = 5   # kill faulty nodes after 5s
                            Thread(target=self._kill_faulty, args=(replica_i,faulty_duration)).start()
                else:
                    print("All replicas are correct")

            elif delay > 0 and faults == 0:
                with open('delay.json') as f:
                    delay_config = json.load(f)
                    f.close()
                if delay_config[f'{node_i}'][0] == 1:
                    Thread(target=self._delay, args=(node_i, delay_config[f'{node_i}'][1], delay_config[f'{node_i}'][2])).start()
           
            elif partition == True and faults == 0 and delay == 0:
                with open('partition.json') as f:
                    partition_config = json.load(f)
                    f.close()
                    targets = []
                    for node in range(servers):
                        if partition_config[f'{node_i}'][0] != partition_config[f'{node}'][0]:
                            targets.append(ips[node])
                    Thread(target=self._partition, args=(targets, partition_config[f'{node_i}'][1], partition_config[f'{node_i}'][2])).start()
            

            sleep(duration+2) # 2s more because clients sleep 2s before sending tx
            self._kill_nodes()
            
            # Parse logs and return the parser.
            
            
            return LogParser.process(PathMaker.logs_path(), faults=faults)
           

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)

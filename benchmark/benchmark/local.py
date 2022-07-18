# Copyright(C) Facebook, Inc. and its affiliates.
from operator import index
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep
import json
from threading import Thread

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
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
    
    def _kill_faulty(id, duration):
        print(f'server {id} is faulty and will be crashed after {duration} s')
        sleep(duration)
        subprocess.run(['tmux', 'kill-session', '-t', f'client-{id}-0'])
        subprocess.run(['tmux', 'kill-session', '-t', f'primary-{id}'])
        subprocess.run(['tmux', 'kill-session', '-t', f'worker-{id}-0'])

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            Print.info('Reading configuration')
            with open('config.json') as f:
                config = json.load(f)
            read = 1 
            
            nodes, rate, replicas, servers, local = self.nodes[0], self.rate[0], self.replicas, self.servers, self.local
            if read == 1:
                replicas = config['replicas']
                servers = config['servers']
                local = config['local'] 
                duration = config['duration']  
                rate = config['input_rate'] 
                faults = config['faults']  

            f.close() 
            nodes = replicas * servers

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            print("For the first running, compilation halts a little longer")
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
                # if local == 1:
                #     subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]
            node_i = int(subprocess.check_output(['tail', '-1', 'index.txt']))
            node_ip = '127.0.0.1'
            match node_i:
                case 0: node_ip = '129.13.88.182'
                case 1: node_ip = '129.13.88.183'
                case 2: node_ip = '129.13.88.184'
                case 3: node_ip = '129.13.88.185'
                case 4: node_ip = '129.13.88.186'
                case 5: node_ip = '129.13.88.187'
                case 6: node_ip = '129.13.88.188'
                case 7: node_ip = '129.13.88.189'
                case 8: node_ip = '129.13.88.190' 
                case 9: node_ip = '129.13.88.180'

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers, local, servers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())
            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses(self.faults)
            rate_share = ceil(rate / committee.workers())
            if local == 0:
                for i, addresses in enumerate(workers_addresses):
                    for (id, address) in addresses:
                        addr_ip = address[:-5]
                        if node_ip == addr_ip:
                            print("!!!!!!!!!!!!!!!")
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
            if local == 1:
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
            if local == 0:
                for i, address in enumerate(committee.primary_addresses(self.faults)):
                    if node_i == i % servers:
                        cmd = CommandMaker.run_primary(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            PathMaker.db_path(i),
                            PathMaker.parameters_file(),
                            debug=debug
                        )
                        print("cmd for primaries")
                        print(cmd)
                        log_file = PathMaker.primary_log_file(i)
                        self._background_run(cmd, log_file)
            if local == 1:
                for i, address in enumerate(committee.primary_addresses(self.faults)):
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
            if local == 0:
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
                            print("cmd for works")
                            print(cmd)
                            log_file = PathMaker.worker_log_file(i, id)
                            self._background_run(cmd, log_file)
            if local == 1:
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

            # Wait for all transactions to be processed.
            with open('faulty.json','r') as f:
                faulty_config = json.load(f)
                f.close()
            
            Print.info(f'Running benchmark ({duration} sec)...')
            #print(faulty_config[f'{node_i}'][0])
            if faulty_config[f'{node_i}'][0] == 1:
                # print(f'This server mpc-{node_i} is faulty')
                # sleep(faulty_config[f'{node_i}'][1])
                # self._kill_faulty(node_i)
                # #print(f'kill faulty replicas after {faulty_config[{node_i}][1]}s')
                # print(f'This server mpc-{node_i} is crashed')
                Thread.start(self._kill_faulty, (node_i,faulty_config[f'{node_i}'][1]))
            else:
                sleep(duration)
                self._kill_nodes()

            # Parse logs and return the parser.
            
            return LogParser.process(PathMaker.logs_path(), faults=faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)

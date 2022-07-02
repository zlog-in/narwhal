# Copyright(C) Facebook, Inc. and its affiliates.
from audioop import add
from platform import node
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

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
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]   #self is the fabfile.py
            print("remove logs")
            
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            #cmd = f'{CommandMaker.clean_logs()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            # Removing the store may take time.
            sleep(0.5)
            
            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            local = int(input("Is it local?"))
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]  # .node-i.json
            #print("key_files:") ['.node-0.json', '.node-1.json', '.node-2.json', '.node-3.json']
            #print(key_files)
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                if local:
                    subprocess.run(cmd, check=True)    # ./node generate_keys --filename .node-0.json
                    print(local)
                keys += [Key.from_file(filename)]

            node_i = subprocess.check_output(['tail', '-1', 'index.txt'])
            node_ip = '127.0.0.1'
            match int(node_i):
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
            print(node_ip)

            #print(f'name and key for node {node_i}:')
            names = [x.name for x in keys]
            secrets = [x.secret for x in keys]
            #print(names[node_i])
            #print(secrets[node_i])
            
            #sleep(3)
            committee = LocalCommittee(names, self.BASE_PORT, self.workers, nodes, local)
            committee.print(PathMaker.committee_file())
            #sleep(3)
            #Z
           
            self.node_parameters.print(PathMaker.parameters_file())

            if local == 1:
                # Run the clients (they will wait for the nodes to be ready).
                workers_addresses = committee.workers_addresses(self.faults)
                
                print("workers_address")
                print(workers_addresses)
                rate_share = ceil(rate / committee.workers())
                for i, addresses in enumerate(workers_addresses):
                    #print("addresses:")
                    #print(addresses)
                    for (id, address) in addresses:
                        print("worker address")
                        print(address)
                        cmd = CommandMaker.run_client(
                            address,
                            self.tx_size,
                            rate_share,
                            [x for y in workers_addresses for _, x in y] 
                        )
                        #print("cmd for running client")
                        #print(cmd)
                        log_file = PathMaker.client_log_file(i, id)
                        self._background_run(cmd, log_file)
                

                
                # Run the primaries (except the faulty ones).
                for i, address in enumerate(committee.primary_addresses(self.faults)):
                    cmd = CommandMaker.run_primary(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i),
                        PathMaker.parameters_file(),
                        debug=debug
                    )
                    #print("cmd for running primaries")
                    #print(cmd)
                    log_file = PathMaker.primary_log_file(i)
                    self._background_run(cmd, log_file)

                # Run the workers (except the faulty ones).
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
                        #print("cmd for works")
                        #print(cmd)
                        log_file = PathMaker.worker_log_file(i, id)
                        self._background_run(cmd, log_file)

            if local == 0:
                # Run the a client (they will wait for the nodes to be ready).
                workers_addresses = committee.workers_addresses(self.faults)
                
                print("workers_address")
                print(workers_addresses)
                rate_share = ceil(rate / committee.workers())
                for i, addresses in enumerate(workers_addresses):
                    #print("addresses:")
                    #print(addresses)
                    for (id, address) in addresses:
                        #print("worker address")
                        #print(address)   # ip address without port
                        #print("node_ip")
                        #print(node_ip)
                        selected_ip = address[:-5]
                        
                        
                        #print("selected_ip")
                        #print(selected_ip)
                        if node_ip == selected_ip:
                            print("!!!!!!!!!!!!!!!")
                            cmd = CommandMaker.run_client(
                                address,
                                self.tx_size,
                                rate_share,
                                [x for y in workers_addresses for _, x in y] 
                            )
                            print(f"cmd for running client on node {node_ip}")
                            print(cmd)
                            log_file = PathMaker.client_log_file(i, id)
                            self._background_run(cmd, log_file)
                

                
                # Run the primaries (except the faulty ones).
                for i, address in enumerate(committee.primary_addresses(self.faults)):
                    print(f'primary index: {i}')
                    if i == node_i:
                        cmd = CommandMaker.run_primary(
                            PathMaker.key_file(i),
                            PathMaker.committee_file(),
                            PathMaker.db_path(i),
                            PathMaker.parameters_file(),
                            debug=debug
                        )
                        print("cmd for running primaries")
                        print(cmd)
                        log_file = PathMaker.primary_log_file(i)
                        self._background_run(cmd, log_file)

                # Run the workers (except the faulty ones).
                for i, addresses in enumerate(workers_addresses):
                    print(f'work index {i}')
            
                    if node_i == i:
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

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            
            self._kill_nodes()
            
            print("Benchmarking ends")
            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)

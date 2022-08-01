# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean
from datetime import datetime
import sqlite3
from benchmark.utils import Print
import json

class ParseError(Exception):
    pass

with open('index.txt') as f:
    NODE_I = int(f.readline())
    f.close()


with open('config.json') as f:
    config = json.load(f)
    f.close()
PARSING = config['parsing']
DURATION = config['duration']


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'
        

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        
        self.misses = sum(misses)

        
        # print(f'self.sent_samples: {self.sent_samples}')

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        
        proposals, commits, self.configs, primary_ips = zip(*results)

      
        self.proposals = self._merge_results([x.items() for x in proposals])
  
        self.commits = self._merge_results([x.items() for x in commits])
        # print(f'self.commits: {self.commits}')
        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        # print(f'sizes: {sizes}')
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)
        
        # print(f'self.received_sample: {self.received_samples}')
        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )
        
        # print("__init__ ends")

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')
        # something missing in log files
        size = int(search(r'Transactions size: (\d+)', log).group(1))  # () indicates a group that whatever regex matched inside is.
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)   # timestap of starting sending tx 
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}  # timestap for sending each tx

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {d: int(s) for d, s in tmp}
        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): d for d, s in tmp}
        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return sizes, samples, ip  # received samples

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        # print("consensus tps")
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        if PARSING == 1:
            with open(f'./logs/result-{NODE_I}.json') as f:
                result = json.load(f)
                f.close()
            result.update({'consensus_start': start, 'consensus_end': end, 'consensus_bytes': bytes, 'consensus_size': self.size[0], 'consensus_bps': bps, 'consensus_tps': tps})

            with open(f'./logs/result-{NODE_I}.json', 'w') as f:
                json.dump(result, f, indent=4)
                f.close()

        return tps, bps, duration

    def _consensus_latency(self):
        # print("_consensu_latency")
        if PARSING == 0:
            latency = [c - self.proposals[d] for d, c in self.commits.items()]
        if PARSING == 1:
            latency = [c - self.proposals[d] for d, c in self.commits.items() if d in self.proposals]
            with open(f'./logs/result-{NODE_I}.json') as f:
                result = json.load(f)
                f.close()
                result.update({'consensus_latency': mean(latency) * 1000})
            with open(f'./logs/result-{NODE_I}.json', 'w') as f:
                json.dump(result, f, indent=4)
               
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        # print("end to end tps")
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        if PARSING == 1:
            with open(f'./logs/result-{NODE_I}.json') as f:
                result = json.load(f)
                f.close()
            result.update({'end2end_start': start, 'end2end_end': end, 'end2end_bytes': bytes, 'end2end_size': self.size[0], 'end2end_bps': bps, 'end2end_tps':tps})

            with open(f'./logs/result-{NODE_I}.json', 'w') as f:
                json.dump(result, f, indent=4)
                f.close()
        return tps, bps, duration

    def _end_to_end_latency(self):
        # print("end to end latency")
        
            

        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
                    # print(latency)
        
        if PARSING == 1:
            with open(f'./logs/result-{NODE_I}.json') as f:
                result = json.load(f)
                f.close()
            result.update({'end2end_latency': mean(latency)*1000})
            
            with open(f'./logs/result-{NODE_I}.json', 'w') as f:
                json.dump(result, f, indent=4)
                f.close()

        return mean(latency) if latency else 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

        with open('config.json') as f:
            config = json.load(f)
        
        replicas = config['replicas']
        servers = config['servers']
        local = config['local'] 
        duration = config['duration']  
        rate = config['input_rate'] 
        faults = config['faults']   
        nodes = replicas * servers
        f.close()

        with open('faulty.json') as f:
            faulty_config = json.load(f)
            f.close()
        time_seed = faulty_config['time_seed']
        results_db = sqlite3.connect('./mpc/results.db')
        insert_S2Narwhal_results = f'INSERT INTO S2Narwhal VALUES ("{time_seed}", {local}, {nodes}, {faults}, {duration}, {rate}, {round(consensus_tps)}, {round(consensus_latency)}, {round(end_to_end_latency)})'
        results_db.cursor().execute(insert_S2Narwhal_results)
        results_db.commit()
        results_db.close()
       
        # add results to sqlite

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {faults} node(s)\n'
            f' Committee size: {nodes} node(s)\n'
            f' Worker(s) per node: 1 worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    # def print(self, filename):
    #     assert isinstance(filename, str)
    #     with open(filename, 'a') as f:
    #         f.write(self.result())

    def remote_result(self):
        # print("reults printed")
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']
        

        with open(f'./logs/result-{NODE_I}.json', 'w') as f:
            json.dump({'header_size': int(header_size), 'max_header_delay':int(max_header_delay), 'gc_depth': int(gc_depth), 'sync_retry_delay': int(sync_retry_delay), 'sync_retry_nodes': int(sync_retry_nodes), 'batch_size': int(batch_size), 'max_batch_delay': int(max_batch_delay) }, f, indent=4)
            f.close()

        self._consensus_latency()
        self._consensus_throughput()
        self._end_to_end_throughput()
        self._end_to_end_latency()

        print('Remote results are summarized')



    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)
        # print(cls)
        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            # print(filename)
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]
        # cls means class, which is the first parameters of a class method.
        # clients, primaries and workers are the names of all logs under folder log
        # print("process ends")
        
        return cls(clients, primaries, workers, faults=faults)

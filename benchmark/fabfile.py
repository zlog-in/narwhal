# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError
import json


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 30_000,
        'tx_size': 512,
        'duration': 60,
        'replicas': 10,
        'servers': 10,
        'local': 0,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 10,  # number of nodes  ??? what is the number of sync retry nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
    

        with open('bench_parameters.json') as f:
            bench_parameters = json.load(f)
            f.close()
        with open('node_parameters.json') as f:
            node_parameters = json.load(f)
            f.close()
        
        if bench_parameters['local'] == True:
            ret = LocalBench(bench_parameters, node_parameters).run(debug)
            Print.info('Parsing logs...')
            print(ret.result())
        elif bench_parameters['local'] == False and bench_parameters['parsing'] == False:
            LocalBench(bench_parameters,node_parameters).run(debug)
            print("Parsing logs locally")
        elif bench_parameters['local'] == False and bench_parameters['parsing'] == True:\
            ret = LocalBench(bench_parameters,node_parameters).run(debug)
            print(ret.remote_result())
        
        
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200  # ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        
        print(LogParser.process('./logs', faults='?').result())
        

        # print(LogParser.process('./logs', node_i, faults='?').result())

    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))

@task 
def parsing_remote(ctx):
    ''' Parsing logs remotely '''
    try:
        with open('config.json') as f:
            config = json.load(f)
        f.close()
        if config['parsing'] == 1:
            print(LogParser.process('./logs', faults='?').remote_result())

        # print(LogParser.process('./logs', node_i, faults='?').result())

    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


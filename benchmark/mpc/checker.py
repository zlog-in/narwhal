import os
import json



bench_parameters = {
    "nodes": 4,
    "workers": 1,
    "rate": 30000,
    "tx_size": 512,
    "duration": 50,
    "delay": 0,
    "replicas": 4,
    "faults": 0,
    "servers": 10,
    "local": False,
    "parsing": True
}



node_parameters = {
    "header_size": 1000,  
    "max_header_delay": 200,  
    "gc_depth": 50,  
    "sync_retry_delay": 1000,  
    "sync_retry_nodes": 10,  
    "batch_size": 500000,  
    "max_batch_delay": 200  
}

with open('../node_parameters.json', 'w') as f:
    json.dump(node_parameters, f, indent=4)
    f.close()


scenarios = ["S1"]

for scenario in scenarios:

    if scenario == "S1":
    

        replicas = [1]
        rates = [20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]
        durations = [20, 40, 60, 80, 100, 120, 140]
        round = 10

        # replicas = [1,2,3,4,5,6]
        # rates = [20000, 30000, 40000, 50000,60000]
        # rate = 20
        # time = 16.7 Hour

        for rep in replicas:
            bench_parameters['replicas'] = rep
            for rat in rates:
                bench_parameters['rate'] = rat
                for dur in durations:
                    bench_parameters['duration'] = dur
                    with open('../bench_parameters.json', 'w') as f:
                            json.dump(bench_parameters, f, indent=4)
                            f.close()
                    for r in range(round):
                        os.system('fab faulty')
                        os.system('fab getresult')

    elif scenario == "S2":
        replicas = [1]
        rates = [20000]
        round = 2
        # replicas = [1,2,3,4,5,6]
        # rates = [20000, 30000, 40000, 50000,60000]
        # rate = 20
        # time = 16.7 Hour
        # number of f: 1-f
        bench_parameters['delay'] = 0

        for rep in replicas:
            bench_parameters['replicas'] = rep
            bench_parameters['faults'] = rep*3 + (rep-1)//3
            
            for rat in rates:
                    bench_parameters['rate'] = rat
                    with open('../bench_parameters.json', 'w') as f:
                            json.dump(bench_parameters, f, indent=4)
                            f.close()
                    
                    for r in range(round):
                        os.system('fab faulty')
                        os.system('fab getresult')

if scenario == "S3":
    print("S3")
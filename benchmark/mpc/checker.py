import os
import json



bench_parameters = {
    "nodes": 4,
    "workers": 1,
    "rate": 120000,
    "tx_size": 512,
    "duration": 20,
    "delay": 0,
    "replicas": 1,
    "faults": 0,
    "servers": 10,
    "local": False,
    "parsing": False,
    "partition": False,
    "S2f": True
}



node_parameters = {
    "header_size": 100,    
    "max_header_delay": 25,  
    "gc_depth": 50,  
    "sync_retry_delay": 3000,  
    "sync_retry_nodes": 10,  
    "batch_size": 513,  # in bytes
    "max_batch_delay": 1000 # 1000  
}

with open('../node_parameters.json', 'w') as f:
    json.dump(node_parameters, f, indent=4)
    f.close()


scenarios = ["S1", "S2", "S2f"]

for scenario in scenarios:

    
    if scenario == "S1":
        bench_parameters['delay'] = 0
        bench_parameters['faults'] = 0
        bench_parameters['S2f'] = False
   


        # replicas = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        # rates = [4000, 5000, 6000, 7000, 8000, 9000, 10000]
        # round = 20

        replicas = [1]
        rates = [10000]
        round = 1
      
        for rep in replicas:
            bench_parameters['replicas'] = rep
            for rat in rates:
                bench_parameters['rate'] = rat
                with open('../bench_parameters.json', 'w') as f:
                        json.dump(bench_parameters, f, indent=4)
                        f.close()
                for r in range(round):
                    os.system('fab faulty')
                    os.system('fab parsing')

    elif scenario == "S2":
        bench_parameters['delay'] = 0
        bench_parameters['S2f'] = False
        replicas = [1]
        rates = [10000]
        round = 1
        # replicas = [1,2,3,4,5,6]
        # rates = [20000, 30000, 40000, 50000,60000]
        # rate = 20
        # time = 16.7 Hour
        # number of f: 1-f
        

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
                        os.system('fab parsing')

    elif scenario == "S2f":
        bench_parameters['delay'] = 0
        bench_parameters['S2f'] = True
        replicas = [1]
        rates = [10000]
        round = 1

        for rep in replicas:
            bench_parameters['replicas'] = rep
            faults = rep*3 + (rep-1)//3
            
            for f in range(faults+1):
                bench_parameters['faults'] = f
                for rat in rates:
                        bench_parameters['rate'] = rat
                        with open('../bench_parameters.json', 'w') as f:
                                json.dump(bench_parameters, f, indent=4)
                                f.close()
                        
                        for r in range(round):
                            os.system('fab faulty')
                            os.system('fab parsing')


    elif scenario == "S3":
        bench_parameters['faults'] = 0
        
        replicas = [4,5,6]
        rates = [40000, 50000,60000]
        delays = [1000, 2000, 3000, 4000, 5000]
        round = 20
        # replicas = [1,2,3,4,5,6]
        # rates = [20000, 30000, 40000, 50000,60000]
        # rate = 20
        # time = 16.7 Hour
        # number of f: 1-f
        

        for rep in replicas:
            bench_parameters['replicas'] = rep
            
            for rat in rates:
                    bench_parameters['rate'] = rat
                    for dela in delays:
                        bench_parameters['delay'] = dela
                        with open('../bench_parameters.json', 'w') as f:
                                json.dump(bench_parameters, f, indent=4)
                                f.close()
                        
                        for r in range(round):
                            os.system('fab timeout')
                            os.system('fab parsing')


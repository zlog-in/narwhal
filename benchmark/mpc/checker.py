import os
import json



bench_parameters = {
    "nodes": 4,
    "workers": 1,
    "rate": 120000,
    "tx_size": 512,
    "duration": 50,
    "delay": 0,
    "replicas": 1,
    "faults": 0,
    "servers": 10,
    "local": False,
    "parsing": False,
    "partition": False,
    "S2f": False,
    "S3_delay": False
}



node_parameters = {
    "header_size": 1000,    #100
    "max_header_delay": 500,  
    "gc_depth": 50,  
    "sync_retry_delay":10000,  
    "sync_retry_nodes": 10,  
    "batch_size": 520,  # 513 in bytes 500000
    "max_batch_delay": 10000 # 1000  
}

with open('../node_parameters.json', 'w') as f:
    json.dump(node_parameters, f, indent=4)
    f.close()


scenarios = ["S2"]

for scenario in scenarios:

    
    if scenario == "S1":
        bench_parameters['delay'] = 0
        bench_parameters['faults'] = 0
        bench_parameters['S2f'] = False
        bench_parameters['S3_delay'] = False
   


        # replicas = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        # rates = [4000, 5000, 6000, 7000, 8000, 9000, 10000]
        # round = 20

        replicas = [1]
        rates = [250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 3750, 4000, 4250, 4500, 4750, 5000]
        round = 10
       
        durations = [150, 200]
        
        for dur in durations:
            bench_parameters['duration'] = dur
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
        bench_parameters['S3_delay'] = False
        replicas = [4]
        rates = [10000]
        round = 20
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
        bench_parameters['S3_delay'] = False
        replicas = [5]
        rates = [10000]
        round = 1

        for rep in replicas:
            bench_parameters['replicas'] = rep
            faults = rep*3 + (rep-1)//3
            # for f in range(faults - rep//2, faults+1):
            for f in range(faults-5, faults + 1):
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
        bench_parameters['S2f'] = False
        bench_parameters['S3_delay'] = True
        # replicas = [1,3, 4, 5]
        # rates = [2000]
        # delays = [0, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
        # round = 10

        replicas = [1, 3, 5, 7, 10]
        rates = [4000]
        delays = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
        round = 15
    
        bench_parameters['duration'] = 50

        for rep in replicas:
            bench_parameters['replicas'] = rep
            # node_parameters['sync_retry_nodes'] = rep * 10 - (rep*3 + (rep-1)//3)
            for rat in rates:
                    bench_parameters['rate'] = rat
                    for dela in delays:
                        bench_parameters['delay'] = dela
                        # node_parameters['sync_retry_delay'] = dela * 3
                        with open('../bench_parameters.json', 'w') as f:
                                json.dump(bench_parameters, f, indent=4)
                                f.close()

                        # with open('../node_parameters.json', 'w') as f:
                        #         json.dump(node_parameters, f, indent=4)
                        #         f.close()
                        
                        for r in range(round):
                            os.system('fab timeout')
                            os.system('fab parsing')




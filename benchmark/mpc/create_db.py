import sqlite3
from datetime import datetime

db = 'results.db'
results_db = sqlite3.connect(db)

creat_scenario1_narwhal = """CREATE TABLE IF NOT EXISTS S1Narwhal (
                         Date text,
                         Local integer,
                         Replicas integer,
                         Faults integer,
                         Duration integer,
                         Input_Rate integer,
                         TPS integer,
                         Consensus_Latency integer,
                         E2E_Latency integer
                         )
                         """
results_db.cursor().execute(creat_scenario1_narwhal)


creat_scenario2_narwhal = """CREATE TABLE IF NOT EXISTS S2Narwhal (
                         Date text,
                         Local integer,
                         Replicas integer,
                         Faults integer,
                         Duration integer,
                         Input_Rate integer,
                         TPS integer,
                         Consensus_Latency integer,
                         E2E_Latency integer
                         )
                         """
results_db.cursor().execute(creat_scenario2_narwhal)

creat_scenario2f_narwhal = """CREATE TABLE IF NOT EXISTS S2FNarwhal (
                         Date text,
                         Local integer,
                         Replicas integer,
                         Faults integer,
                         Duration integer,
                         Input_Rate integer,
                         TPS integer,
                         Consensus_Latency integer,
                         E2E_Latency integer
                         )
                         """
results_db.cursor().execute(creat_scenario2f_narwhal)

creat_scenario3_narwhal = """CREATE TABLE IF NOT EXISTS S3Narwhal (
                         Date text,
                         Local integer,
                         Replicas integer,
                         Faults integer,
                         Delay interger,
                         Sync_Retry interger,
                         Duration integer,
                         Input_Rate integer,
                         TPS integer,
                         Consensus_Latency integer,
                         E2E_Latency integer
                         )
                         """
results_db.cursor().execute(creat_scenario3_narwhal)

creat_scenario4_narwhal = """CREATE TABLE IF NOT EXISTS S4Narwhal (
                         Date text,
                         Local integer,
                         Replicas integer,
                         Faults integer,
                         Partition interger,
                         Sync_Retry interger,
                         Duration integer,
                         Input_Rate integer,
                         TPS integer,
                         Consensus_Latency integer,
                         E2E_Latency integer
                         )
                         """
results_db.cursor().execute(creat_scenario4_narwhal)




results_db.cursor().execute("ALTER TABLE S1Narwhal ADD COLUMN gc_depth integer")
results_db.commit()
results_db.close()



import pandas as pd
import time 
import fastavro
import os
from tabulate import tabulate
import matplotlib.pyplot as plt
import numpy as np

#Load CSV Files
def load_csv (file_path):
    start_time = time.time()
    df = pd.read_csv(file_path)
    load_time = time.time() - start_time
    return df , load_time 

#Load Parquet files
def load_Parquet (file_path):
    start_time = time.time()
    df = pd.read_parquet(file_path)
    load_time = time.time() - start_time
    return df , load_time 

#Load Avro files
def load_avro(file_path):
    start_time = time.time()
    with open(file_path, 'rb') as f:  #binary
        df = pd.DataFrame.from_records(fastavro.reader(f))
    load_time = time.time() - start_time
    return df, load_time

def load_all_formats(base_path, format):
    
    files = ["clubs_dim", "merge-Transfer_Fact", "players_dim", "time_dim", "competitions_dim"]

    data = {}
    load_times = {}

    for file in files:
        file_path = os.path.join(base_path, f"{file}.{format}")

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        load_time = None

        if format == 'csv':
            df, load_time = load_csv(file_path)  
        elif format == 'avro':
            df, load_time = load_avro(file_path)  
        elif format == 'parquet':
            df, load_time = load_Parquet(file_path)   
        else:
            raise ValueError("Unsupported format: Please choose 'csv', 'avro', or 'parquet'.")

        data[file] = df
        load_times[file] = load_time

        print(f"Successfully loaded {file_path} in {load_time:.4f} seconds.")

    return data, load_times


 
base_path = 'D:\\DE journey\\DM projects\\Transfer\\all_files'

 
data_csv, load_times_csv = load_all_formats(base_path, format='csv')
 
data_avro, load_times_avro = load_all_formats(base_path, format='avro')
 
data_parquet, load_times_parquet = load_all_formats(base_path, format='parquet')


#Display Load Times
files = ["clubs_dim", "merge-Transfer_Fact", "players_dim", "time_dim", "competitions_dim"]

csv_times = [load_times_csv.get(f, 0) for f in files]
avro_times = [load_times_avro.get(f, 0) for f in files]
parquet_times = [load_times_parquet.get(f, 0) for f in files]

x = np.arange(len(files))  
width = 0.25  

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(x - width, csv_times, width, label='CSV', color='blue')
ax.bar(x, avro_times, width, label='AVRO', color='green')
ax.bar(x + width, parquet_times, width, label='Parquet', color='red')

ax.set_xlabel("Files")
ax.set_ylabel("Load Time (seconds)")
ax.set_title("Comparison of Load Times for Different Formats")
ax.set_xticks(x)
ax.set_xticklabels(files, rotation=20)
ax.legend()

plt.show()


#Get Top Goal Scorers
def query_top_goal_scorers(players_df):
    last_season = players_df['last_season'].max()
    top_goal_scorers = players_df[players_df['last_season'] == last_season].nlargest(10, 'goals')
    return top_goal_scorers[['name', 'goals', 'last_season']]


#Get Top Transfers by Fee
def query_top_transfers_by_fee(transfer_df):
    top_transfers = transfer_df.nlargest(10, 'total_transfer_fee')
    return top_transfers[['player_id', 'from_club_id', 'to_club_id', 'total_transfer_fee']]

def display_results(format, top_goal_scorers, top_transfers):
    print(f"Results for {format} format:")
    print("\nTop 10 Goal Scorers:")
    print(tabulate(top_goal_scorers, headers="keys", tablefmt="pretty"))
    
    
    print("\nTop 10 Transfers by Fee:")
    print(tabulate(top_transfers, headers="keys", tablefmt="pretty"))
    print("\n" + "="*50 + "\n")
    
for format, data in zip(['CSV', 'AVRO', 'Parquet'], [data_csv, data_avro, data_parquet]):
    players_df = data['players_dim']
    top_goal_scorers = query_top_goal_scorers(players_df)
    display_results(format + ' - Top Goal Scorers', top_goal_scorers, None)
    
    
for format, data in zip(['CSV', 'AVRO', 'Parquet'], [data_csv, data_avro, data_parquet]):
    transfer_df = data['merge-Transfer_Fact']
    top_transfers = query_top_transfers_by_fee(transfer_df)
    display_results(format + ' - Top Transfers by Fee', None, top_transfers)
    
    
def measure_query_time(query_func, *args):
    start_time = time.time()
    result = query_func(*args)
    query_time = time.time() - start_time
    return result, query_time

def display_query_results(format, query_name, result, query_time):
    print(f"Results for {format} format - {query_name} (Time: {query_time:.4f} seconds):")
    if result is not None:
        print(tabulate(result, headers="keys", tablefmt="pretty"))
    print("\n" + "="*50 + "\n")
 
for format, data in zip(['CSV', 'AVRO', 'Parquet'], [data_csv, data_avro, data_parquet]):
    players_df = data['players_dim']
    clubs_df = data['clubs_dim']
    transfer_df = data['merge-Transfer_Fact']
    
# Time measurement for scorers query
    top_goal_scorers, goal_scorers_time = measure_query_time(query_top_goal_scorers, players_df)
    display_query_results(format, "Top Goal Scorers", top_goal_scorers, goal_scorers_time)
    
 # Measure time to query highest conversions
    top_transfers, transfers_time = measure_query_time(query_top_transfers_by_fee, transfer_df)
    display_query_results(format, "Top Transfers by Fee", top_transfers, transfers_time)
    
    
def plot_query_times(query_name, csv_time, avro_time, parquet_time):
    formats = ['CSV', 'AVRO', 'Parquet']
    times = [csv_time, avro_time, parquet_time]
    
    plt.figure(figsize=(8, 5))
    plt.bar(formats, times, color=['blue', 'green', 'red'])
    plt.title(f'Query Time Comparison for {query_name}')
    plt.xlabel('Format')
    plt.ylabel('Time (seconds)')
    plt.show()

 
query_times = {
    "Top Goal Scorers": {"CSV": 0, "AVRO": 0, "Parquet": 0},
 
    "Top Transfers by Fee": {"CSV": 0, "AVRO": 0, "Parquet": 0}
}

 
for format, data in zip(['CSV', 'AVRO', 'Parquet'], [data_csv, data_avro, data_parquet]):
    players_df = data['players_dim']
    clubs_df = data['clubs_dim']
    transfer_df = data['merge-Transfer_Fact']
    
 
    top_goal_scorers, goal_scorers_time = measure_query_time(query_top_goal_scorers, players_df)
    query_times["Top Goal Scorers"][format] = goal_scorers_time
    
 
    
 
    top_transfers, transfers_time = measure_query_time(query_top_transfers_by_fee, transfer_df)
    query_times["Top Transfers by Fee"][format] = transfers_time

 
for query_name, times in query_times.items():
    plot_query_times(query_name, times['CSV'], times['AVRO'], times['Parquet'])
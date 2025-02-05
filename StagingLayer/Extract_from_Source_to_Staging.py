import pandas as pd 
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


#--------------------------- connection to db

load_dotenv("pass.env") 

username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@localhost/{database}")

#--------------------------- function to load data in db

def load_data_to_db(file_path , table_name , chunk_size= 10000):
    try:
        with engine.begin() as connection:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunk.to_sql(table_name, con= engine, if_exists = "append" , index = False )
                print(f"insert {len(chunk)} rows into {table_name} Done")
    except SQLAlchemyError as e:
        print(f"Errors occured while inserting data into {table_name}: {e}")
    except FileExistsError:
        print(f"File not exist: {file_path}")
    except Exception as e :
        print(f"error :{e}")
        
#-------------------------------------------
# dictionary inside dictionary

data_files = {
    "Transfer/appearances.csv": {"table_name":"appearances",  "chunk_size":10000},
    "Transfer/club_games.csv": {"table_name": "club_games", "chunk_size": 100000},
    "Transfer/clubs.csv": {"table_name": "clubs", "chunk_size": 10000},
    "Transfer/competitions.csv": {"table_name": "competitions", "chunk_size": 10000},
    "Transfer/game_events.csv": {"table_name": "game_events", "chunk_size": 10000},
    "Transfer/game_lineups.csv": {"table_name": "game_lineups", "chunk_size": 100000},
    "Transfer/games.csv": {"table_name": "games", "chunk_size": 100000},
    "Transfer/player_valuations.csv": {"table_name": "player_valuations", "chunk_size": 100000},
    "Transfer/players.csv": {"table_name": "players", "chunk_size": 100000},
    "Transfer/transfers.csv": {"table_name": "transfer", "chunk_size": 10000},
}

#----------------------------------------------

for file_path, config in data_files.items():
    load_data_to_db(file_path, config["table_name"], config["chunk_size"])
import pandas as pd
import fastavro
import numpy as np
 
 
data_files = {
    "clubs_dim.csv": {
        "avro_file": "clubs_dim.avro",
        "schema": {
            'type': 'record',
            'name': 'Club',
            'fields': [
                {'name': 'club_id', 'type': 'int'},
                {'name': 'name', 'type': 'string'},
                {'name': 'competition_type', 'type': ['string', 'null']},
                {'name': 'competition_country', 'type': ['string', 'null']},
                {'name': 'squad_size', 'type': ['int', 'null']},
                {'name': 'average_age', 'type': ['float', 'null']},
                {'name': 'foreigners_number', 'type': ['int', 'null']},
                {'name': 'foreigners_percentage', 'type': ['float', 'null']},
                {'name': 'national_team_players', 'type': ['int', 'null']},
                {'name': 'last_season', 'type': ['int', 'null']}
            ]
        }
    },
    "competitions_dim.csv": {
        "avro_file": "competitions_dim.avro",
        "schema": {
            'type': 'record',
            'name': 'Competition',
           'fields': [
                {'name': 'competition_id', 'type': 'string'},
                {'name': 'name', 'type': 'string'},
                {'name': 'type', 'type': 'string'},
                {'name': 'country_name', 'type': 'string'},   
                {'name': 'confederation', 'type': 'string'}, 
                {'name': 'is_major_national_league', 'type': 'int'}  # 0 or 1
    ]
        }
    },
    "players_dim.csv": {
        "avro_file": "players_dim.avro",
        "schema": {
            'type': 'record',
            'name': 'Player',
            'fields': [
                {'name': 'player_id', 'type': 'int'},
                {'name': 'name', 'type': 'string'},
                {'name': 'country_of_birth', 'type': ['string', 'null']},
                {'name': 'date_of_birth', 'type': ['string', 'null']},
                {'name': 'position', 'type': ['string', 'null']},
                {'name': 'sub_position', 'type': ['string', 'null']},
                {'name': 'last_season', 'type': ['int', 'null']},
                {'name': 'height_in_cm', 'type': ['float', 'null']},
                {'name': 'current_club_id', 'type': ['int', 'null']},
                {'name': 'current_club_name', 'type': ['string', 'null']},
                {'name': 'country_of_citizenship', 'type': ['string', 'null']},
                {'name': 'agent_name', 'type': ['string', 'null']},
                {'name': 'image_url', 'type': ['string', 'null']},
                {'name': 'url', 'type': ['string', 'null']},
                {'name': 'goals', 'type': ['int', 'null']},
                {'name': 'assists', 'type': ['int', 'null']},
                {'name': 'minutes_played', 'type': ['int', 'null']},
                {'name': 'player_club_domestic_competition_id', 'type': ['string', 'null']}
            ]
        }
    },
    "time_dim.csv": {
        "avro_file": "time_dim.avro",
        "schema": {
            'type': 'record',
            'name': 'Time',
            'fields': [
                {'name': 'time_id', 'type': 'int'},
                {'name': 'full_date', 'type': 'string'},
                {'name': 'year', 'type': 'int'},
                {'name': 'quarter', 'type': 'int'},
                {'name': 'month', 'type': 'int'},
                {'name': 'day', 'type': 'int'}
            ]
        }
    }
}

def convert_csv_to_avro(csv_file, avro_file, schema):
    try:
        
        df = pd.read_csv(csv_file)
 
        df = df.replace({np.nan: None})
 
        if 'country_name' in df.columns:
            df['country_name'] = df['country_name'].fillna('')  # Replace NaN with an empty string

        records = df.to_dict('records')
 
        with open(avro_file, 'wb') as f:
            fastavro.writer(f, schema, records)

        print(f"Successfully converted {csv_file} to {avro_file}!")

    except Exception as e:
        print(f"Error converting {csv_file}: {e}")


 
for csv_file, config in data_files.items():
    convert_csv_to_avro(csv_file, config["avro_file"], config["schema"])

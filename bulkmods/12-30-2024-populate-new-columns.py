"""
This script amends the data incorporated in the database incorporating new columns: 
- original_source_id: Data of this column was stored as place_id in the initial import. 
- place_id: Data of this column will be created at import time.
- source: Abreviation of the data source. On this case, it will be "TGN" for The Getty Vocabularies.
"""

import pandas as pd

columns_list = ["place_id", "place_name", "place_type", "latitude", "longitude", "parent_id", "alternate_names", "created_at", "updated_at"]

def inspect_csv(csv_file):
    with open(csv_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            fields = line.strip().split('\t')
            line2inspect = 2425314
            if i == line2inspect:
                print(fields)
                print(len(fields))
                print(line)

def main(csv_file: str):
    """
    Parameters:
    - csv_file: Absolute path to the csv file containing the data to be amended.
    """
    try:
        df = pd.read_csv(csv_file, 
                         sep="\t", 
                         escapechar="\\",
                         encoding="utf-8", 
                         on_bad_lines="warn",
                         low_memory=False,
                         dtype={
                "place_id": 'Int64',      
                "place_name": str,
                "place_type": str,
                "latitude": 'Float64',     
                "longitude": 'Float64',    
                "parent_id": 'Int64',      
                "alternate_names": str,
                "created_at": str,
                "updated_at": str
            })
        df.columns = columns_list
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return
    
    df["original_source_id"] = df["place_id"]
    df["source"] = "TGN"
    
    df.drop(columns=["place_id"], inplace=True)
    
    destination_file = csv_file.replace(".csv", "_new_columns.csv")
    
    df.to_csv(destination_file, index=False)

if __name__ == "__main__":
    #inspect_csv("raw_data/TGN/tgn.csv")
    main("raw_data/TGN/tgn.csv")
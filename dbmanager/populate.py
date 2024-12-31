import dbmanage as db
from lxml import etree
from glob import glob
import numpy as np
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', filename="logs/populate.log")
logger = logging.getLogger(__name__)


class PopulateTGN:
    def __init__(self, file_list: list[str], raw_data_path: str = None) -> None:
        """
        Initialize the PopulateTGN class.

        Args:
            file_list (list[str]): A list of file paths to process.
            raw_data_path (str, optional): The path to the raw data. Defaults to None.
        """
        self.file_list = file_list
        self.raw_data_path = raw_data_path
        
    def process_file(self, file_path: str) -> None:
        """
        Process a single file.

        Args:
            file_path (str): The path to the file to process.
        """
        # Initialize DB connection
        connection = db.connect_to_db()
        cursor = connection.cursor()
        
        try:
            # Parse with namespace awareness
            context = etree.iterparse(file_path, events=('end',), recover=True)
            
            print(f"Processing {file_path}")
            batch_size = 1000
            current_batch = []
            
            # Get the namespace from the first element
            for event, elem in context:
                namespace = elem.nsmap.get(None)
                if namespace:
                    print(f"Found namespace: {namespace}")
                    break
            
            def safe_find_text(element, path, ns):
                try:
                    ns_path = '/'.join('{' + ns + '}' + part for part in path.split('/'))
                    result = element.find('.//' + ns_path)
                    return result.text if result is not None else None
                except:
                    return None
            
            count = 0
            success_count = 0
            error_count = 0
            
            for event, elem in context:
                try:
                    tag = etree.QName(elem).localname
                    
                    if tag == "Subject":
                        count += 1
                        
                        # Get Place ID (Subject ID)
                        original_source_id = int(elem.get('Subject_ID'))
                        
                        # Get preferred name from Preferred_Term using namespace
                        preferred_term = elem.find('.//{' + namespace + '}Terms/{' + namespace + '}Preferred_Term/{' + namespace + '}Term_Text')
                        place_name = preferred_term.text if preferred_term is not None else None
                        
                        # Get coordinates
                        latitude = float(safe_find_text(elem, 'Coordinates/Standard/Latitude/Decimal', namespace)) if safe_find_text(elem, 'Coordinates/Standard/Latitude/Decimal', namespace) else None
                        longitude = float(safe_find_text(elem, 'Coordinates/Standard/Longitude/Decimal', namespace)) if safe_find_text(elem, 'Coordinates/Standard/Longitude/Decimal', namespace) else None
                        
                        # Get place type
                        place_type_elem = elem.find('.//{' + namespace + '}Place_Types/{' + namespace + '}Preferred_Place_Type/{' + namespace + '}Place_Type_ID')
                        place_type = place_type_elem.text if place_type_elem is not None else None
                        place_type = place_type.split('/')[1] if place_type and '/' in place_type else place_type
                        
                        # Get parent ID
                        parent_elem = elem.find('.//{' + namespace + '}Parent_Relationships/{' + namespace + '}Preferred_Parent/{' + namespace + '}Parent_Subject_ID')
                        parent_id = int(parent_elem.text) if parent_elem is not None and parent_elem.text else None
                        
                        # Get non-preferred terms (alternate names)
                        alternate_names = []
                        for term in elem.findall('.//{' + namespace + '}Terms/{' + namespace + '}Non-Preferred_Term/{' + namespace + '}Term_Text'):
                            if term.text:
                                # clean text before adding to list
                                cleaned_term = term.text.replace('\\', '\\\\')
                                alternate_names.append(cleaned_term.strip())
                        
                        if original_source_id and place_name:
                            current_batch.append((
                                original_source_id,
                                "TGN",
                                place_name,
                                place_type,
                                latitude,
                                longitude,
                                parent_id,
                                "|".join(alternate_names) if alternate_names else None
                            ))
                            success_count += 1
                        
                        # Process batch when it reaches batch_size
                        if len(current_batch) >= batch_size:
                            db.insert_data(cursor, current_batch)
                            connection.commit()
                            print(f"Committed batch of {batch_size} records. Total processed: {count}")
                            current_batch = []
                        
                        # Show progress
                        if count % 1000 == 0:
                            print(f"Processed {count} subjects. Successes: {success_count}, Errors: {error_count}")
                        
                        # Memory management (existing code)
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                            
                except Exception as e:
                    error_count += 1
                    print(f"Error processing record {count}: {str(e)}")
                    break
            
            # Insert any remaining records
            if current_batch:
                db.insert_data(cursor, current_batch)
                connection.commit()
                
            print(f"\nProcess complete!")
            print(f"Total processed: {count}")
            print(f"Successful insertions: {success_count}")
            print(f"Errors: {error_count}")
                
        except Exception as e:
            print(f"Fatal error processing file: {str(e)}")
            raise
        
        finally:
            db.close_db(cursor, connection)


    def populate_db(self):
        """
        Populate the database with the data from the files.
        """
        for file in self.file_list:
            self.process_file(f"{self.raw_data_path if self.raw_data_path else ''}{file}")


class PopulateHGIS:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.cert_map = {
                            "Exacta": 100,
                            "Buena": 85,
                            "Suficiente": 70,
                            "Interpolada": 50,
                            "Geoservice/Satelite": 40,
                            "No localizado": 30,
                            "Identificacion incierta": 25
                        }
        
    def resolve_duplicates(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Resolve duplicates in the dataframe based on 'original_source_id' and 'source'.
        Retains the first occurrence of each duplicate group.
        """
        
        logger.info(f"Found {len(dataframe[dataframe.duplicated(subset=['original_source_id', 'source'], keep=False)])} duplicate rows.")
        
        deduplicated = dataframe.sort_values(by="certainty_score", ascending=False) \
                               .drop_duplicates(subset=["original_source_id", "source"], keep="first")
    
        logger.info(f"Deduplicated DataFrame shape: {deduplicated.shape}")
        
        return deduplicated
    
    def translate_place_types(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Translate the place types from Spanish to English.
        """
        
        TRANSLATION_MAPPING = {
            "Fuerte": "Fort",
            "Parcialidad": "Partial Jurisdiction",
            "Ciudad": "City",
            "Villa": "Town",
            "Pueblo": "Village",
            "Poblacion": "Population Center",
            "Localidad": "Locality",
            "Rural": "Rural Area",
            "[-]": "Unspecified"
        }
        dataframe["place_type"] = dataframe["place_type"].map(TRANSLATION_MAPPING)
        return dataframe
        
    def process_file(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path)
            
            logger.info(f"Original DataFrame shape: {df.shape}")
            logger.info(f"Original columns: {df.columns.tolist()}")
            
            df = df.rename(columns={
                    "gz_id": "original_source_id",
                    "label": "place_name",
                    "categoria": "place_type",
                    "lat": "latitude",
                    "lon": "longitude",
                    "es_parte_de": "parent_id",
                    "variantes": "alternate_names"
                })
            
            df["certainty_score"] = df["cert"].map(self.cert_map)
            
            df["source"] = "HGIS"
            
            df["place_name"] = df["place_name"].fillna('[Unnamed Place]')
            
            numeric_columns = ["latitude", "longitude", "original_source_id", "parent_id"]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
            
            columns = ["original_source_id", "source", "place_name", "place_type", 
                        "latitude", "longitude", "parent_id", "alternate_names", "certainty_score"]
            
            df = df[columns]
            
            # Resolve duplicates
            df = self.resolve_duplicates(df)
            
            df.drop(columns=["certainty_score"], inplace=True)
            
            # Translate place types
            df = self.translate_place_types(df)
            
            logger.info(f"Processed file: {self.file_path}")
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {df.columns.tolist()}")
            
            return df
        
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise

    def populate_db(self) -> None:
        connection = db.connect_to_db()
        cursor = connection.cursor()
        
        try:
            df = self.process_file()
            
            batch_size = 1000
            total_batches = len(df) // batch_size + (1 if len(df) % batch_size else 0)
            
            for i in range(0, len(df), batch_size):
                try:
                    batch = df.iloc[i:i+batch_size].values.tolist()
                    if batch:
                        db.insert_data(cursor, batch)
                        connection.commit()
                        
                    logger.info(f"Inserted batch {i//batch_size + 1} of {total_batches}")
                    
                except Exception as e:
                    logger.error(f"Error in batch {i//batch_size + 1}: {str(e)}")
                    logger.error(f"Problem row sample: {batch[0] if batch else 'No data'}")
                    raise
                
            logger.info("Data insertion completed successfully.")
            logger.info(f"Total records inserted: {len(df)}")
            
        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise
            
        finally:
            db.close_db(cursor, connection)
        

class Reimporter:
    """
    Reimports data from a csv file into the database.
    Only use this if you are sure you want to delete all existing data in the database.
    
    Parameters:
        csv_file (str): The path to the csv file to import.
        table_name (str): The name of the table to import the data into.
        source (str, optional): The source of the data (e.g. "TGN" or "HGIS"). Defaults to None. If None, the source will not be filtered and all data will be deleted.
    """
    def __init__(self, csv_file: str, table_name: str, source: str = None):
        self.csv_file = csv_file
        self.table_name = table_name
        self.source = source
        self.connection = db.connect_to_db()
        self.cursor = self.connection.cursor()


    def prepare_csv(self):
        """
        Prepares the csv file for import.
        """
        df = pd.read_csv(
            self.csv_file,
            escapechar="\\",
            encoding="utf-8",
            na_values=["\\", "N", "NULL", "", "nan", "\\N"],  
            keep_default_na=True, 
            low_memory=False,
        )
        
        logger.info(f"Columns in DataFrame: {df.columns.tolist()}")
        logger.info(f"DataFrame shape: {df.shape}")
        
        df['place_name'] = df['place_name'].fillna('[Unnamed Place]')
        
        if "alternate_names" in df.columns:
            df["alternate_names"] = df["alternate_names"].str.replace(r'\\', '', regex=True)
        
        numeric_columns = ["latitude", "longitude", "original_source_id", "parent_id"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
        
        logger.info(f"Sample of prepared data:\n{df.head()}")
        
        return df
    
    def reimport_data(self):
        try:
            df = self.prepare_csv()
            
            warning = input("This will delete all existing data in the database. Are you sure? (y/n) ")
            
            if warning.lower() in {"y", "yes"}:
                logger.info("Deleting all existing data in the database.")
                
                if self.source:
                    self.cursor.execute(f"DELETE FROM {self.table_name} WHERE source = '{self.source}'")
                else:
                    self.cursor.execute(f"DELETE FROM {self.table_name}")
                self.connection.commit()
                
                expected_columns = [
                    'place_name', 'place_type', 'latitude', 'longitude', 
                    'parent_id', 'alternate_names', 'created_at', 'updated_at',
                    'original_source_id', 'source'
                ]
                
                df = df[expected_columns]
                
                logger.info(f"Inserting new data from {self.csv_file}")
                
                values = df.values.tolist()
                
                columns = df.columns.tolist()
                placeholders = ', '.join(['%s'] * len(columns))
                sql = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                logger.info(f"SQL Statement: {sql}")
                logger.info(f"Number of columns in SQL: {len(columns)}")
                logger.info(f"Sample row length: {len(values[0]) if values else 0}")
                
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    try:
                        self.cursor.executemany(sql, batch)
                        self.connection.commit()
                        logger.info(f"Inserted batch {i//batch_size + 1} of {len(values)//batch_size + 1}")
                    except Exception as e:
                        logger.error(f"Error in batch {i//batch_size + 1}: {str(e)}")
                        logger.error(f"Problem row sample: {batch[0] if batch else 'No data'}")
                        raise
                
                logger.info("Data reimport completed successfully.")
            else:
                logger.info("Operation cancelled by user.")
                
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
        finally:
            self.cursor.close()
            self.connection.close()
            self.connection.close()


if __name__ == "__main__":
    #xmlfiles = glob("raw_data/TGN/*.xml")
    #PopulateTGN(xmlfiles).populate_db()
    PopulateHGIS("raw_data/HGIS/gz_info_1.csv").populate_db()
    #Reimporter("raw_data/TGN/tgn_new_columns.csv", "places", "TGN").reimport_data()

import dbmanage as db
from lxml import etree
from glob import glob

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
                        subject_id = int(elem.get('Subject_ID'))
                        
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
                                alternate_names.append(term.text)
                        
                        if subject_id and place_name:
                            current_batch.append((
                                subject_id,
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
                    continue
            
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




if __name__ == "__main__":
    file_list = glob("raw_data/TGN/*.xml")
    populate_db = PopulateTGN(file_list)
    populate_db.populate_db()

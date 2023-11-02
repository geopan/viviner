from data_bridges import DataBridge
import psycopg2
import json
import concurrent.futures
from tqdm import tqdm

# https://api.vivino.com/grapes/1
# Explore deeper - I guess separate grape_detail table - only when has_detailed_info true


class GrapesDataBridge(DataBridge):

    def __init__(self, conn):
        super().__init__(conn)
        self.grapes = []
        
    def create_table_if_not_exist(self):
        query = """
            CREATE TABLE IF NOT EXISTS grapes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                description TEXT,
                seo_name TEXT,
                flavor_profile TEXT,
                color INTEGER,
                acidity INTEGER,
                body INTEGER,
                acidity_description TEXT,
                body_description TEXT,
                top_types JSONB,
                parent_grape_id INTEGER
            );
        """

        try:
            self.cursor.execute(query)
            self.conn.commit()

        except psycopg2.Error as e:
            print(f"An error occurred: {e}")


    def fetch_grapes(self) -> list:
        res = self.r.get(f'grapes')
        self.grapes = res.json()
        return self.grapes
    
    def fetch_grape_by_id(self, grape_id) -> list:
        res = self.r.get(f'grapes/{grape_id}')
        return res.json()

    def insert_grape(self, grape):

        query = """
            INSERT INTO grapes (
                    id, 
                    name, 
                    description,
                    has_detailed_info,
                    seo_name, 
                    flavor_profile,
                    color,
                    acidity,
                    body,
                    acidity_description,
                    body_description,
                    top_types, 
                    parent_grape_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
            ON CONFLICT(id)
            DO UPDATE SET
                name = excluded.name, 
                description = excluded.description,
                has_detailed_info = excluded.has_detailed_info,
                seo_name = excluded.seo_name,
                flavor_profile = excluded.flavor_profile,
                color = excluded.color,
                acidity = excluded.acidity,
                body = excluded.body,
                acidity_description = excluded.acidity_description,
                body_description = excluded.body_description,
                top_types = excluded.top_types,
                parent_grape_id = excluded.parent_grape_id
            """

        try:

            values = (
                grape.get('id'),
                grape.get('name'),
                grape.get('description'),
                grape.get('has_detailed_info'),
                grape.get('seo_name'),
                grape.get('flavor_profile'),
                grape.get('color'),
                grape.get('acidity'),
                grape.get('body'),
                grape.get('acidity_description'),
                grape.get('body_description'),
                json.dumps(grape.get('top_types')),
                grape.get('parent_grape_id')
            )

            self.cursor.execute(query, values)

        except AttributeError as e:
            print(f"An AttributeError occurred: {e}")

        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def insert_grapes(self, grapes=None):

        if grapes is None:
            grapes = self.grapes

        for g in grapes:
            self.insert_grape(g)

    def import_grapes(self):
        self.fetch_grapes()

        # # tqdm is just for a little progress bar
        # for grape in tqdm(self.grapes):
        #     grape_details = self.fetch_grape_by_id(grape['id'])
        #     self.insert_grape(grape_details)

        # Define the function to fetch and insert grapes in parallel
        def process_grape(grape):
            grape_details = self.fetch_grape_by_id(grape['id'])
            self.insert_grape(grape_details)

        # Using ThreadPoolExecutor for concurrent execution
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Wrap the executor with tqdm for a progress bar
            list(tqdm(executor.map(process_grape, self.grapes), total=len(self.grapes)))

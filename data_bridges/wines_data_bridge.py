import psycopg2
import json
from tqdm import tqdm
from data_bridges import DataBridge

# https://api.vivino.com/wineries/254665/wines?per_page=50&sort=best_rated&include_all_vintages=true&language=en

# https://api.vivino.com/wines/${id}
# https://api.vivino.com/wines/${id}/tastes
# https://api.vivino.com/wines/${id}/reviews/_ranked
# https://api.vivino.com/vintages/${vintage.id}


class WinesDataBridge(DataBridge):

    def __init__(self, conn):
        super().__init__(conn)
        self.wines_ids = []

    def load_wines_ids(self):
        with open('list.txt', 'r') as file:
            # Loop over each line in the file
            for line in file:
                # Strip leading/trailing whitespace and convert to integer
                id = int(line.strip())
                self.wines_ids.append(id)

    def fetch_wines_by_winery_id(self, winery_id: str) -> list:

        # Defines the payload, i.e., filters to be used on the search
        payload = {
            'per_page': 50,
            'sort': 'best_rated',
            'include_all_vintages': True,
            'language': 'en'
        }

        res = self.r.get(f'wineries/{winery_id}/wines', params=payload)

        return res.json().get('wines', [])

    def fetch_wine_by_id(self, wine_id: str):
        res = self.r.get(f'wines/{wine_id}')
        return res.json()

    def insert_wine(self, wine):
        query = """
            INSERT INTO wines (
                id, 
                name, 
                seo_name, 
                type_id, 
                vintage_type, 
                is_natural, 
                winery_id, 
                region_id, 
                review_status, 
                statistics
            )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            ON CONFLICT (id)
            DO UPDATE SET
                name = excluded.name, 
                seo_name = excluded.seo_name, 
                type_id = excluded.type_id, 
                vintage_type = excluded.vintage_type, 
                is_natural = excluded.is_natural, 
                winery_id = excluded.winery_id, 
                region_id = excluded.region_id, 
                review_status = excluded.review_status, 
                statistics = excluded.statistics

            """

        try:

            values = (
                wine.get('id'),
                wine.get('name'),
                wine.get('seo_name'),
                wine.get('type_id'),
                wine.get('vintage_type'),
                wine.get('is_natural'),
                wine.get('winery').get('id'),
                wine.get('region').get('id'),
                wine.get('review_status'),
                json.dumps(wine.get('statistics')),
            )

            self.cursor.execute(query, values)

        except AttributeError as e:
            print(f"An AttributeError occurred: {e}")

        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def import_wines(self):
        self.load_wines_ids()

        # tqdm is just for a little progress bar
        for wine_id in tqdm(self.wines_ids):
            wine = self.fetch_wine_by_id(wine_id)
            self.insert_wine(wine)

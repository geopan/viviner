import json
import psycopg2
from tqdm import tqdm
from data_bridges import DataBridge


class WineriesDataBridge(DataBridge):

    def __init__(self, conn):
        super().__init__(conn)
        self.wineries_ids = []

    def load_wineries_ids(self):
        with open('wineries_id_list.txt', 'r') as file:
            # Loop over each line in the file
            for line in file:
                # Strip leading/trailing whitespace and convert to integer
                id = int(line.strip())
                self.wineries_ids.append(id)

    def fetch_winerie_by_id(self, winery_id: str):
        res = self.r.get(f'wineries/{winery_id}')
        return res.json()

    def insert_winery(self, winery):
        query = """
            INSERT INTO wineries (
                id, 
                name,
                seo_name,
                status,
                review_status,
                statistics,
                business_name,
                description,
                specialists_notes,
                phone,
                email,
                facebook,
                instagram,
                is_claimed,
                twitter,
                website,
                winemaker,
                address,
                location,
                region_id)
                VALUES (
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s
                )
            ON CONFLICT (id)
            DO UPDATE SET
                name = excluded.name,
                seo_name = excluded.seo_name,
                status = excluded.status,
                review_status = excluded.review_status,
                statistics = excluded.statistics,
                business_name = excluded.business_name,
                description = excluded.description,
                specialists_notes = excluded.specialists_notes,
                phone = excluded.phone,
                email = excluded.email,
                facebook = excluded.facebook,
                instagram = excluded.instagram,
                is_claimed = excluded.is_claimed,
                twitter = excluded.twitter,
                website = excluded.website,
                winemaker = excluded.winemaker,
                address = excluded.address,
                location = excluded.location,
                region_id = excluded.region_id

            """

        try:

            values = (
                winery.get('id'),
                winery.get('name'),
                winery.get('seo_name'),
                winery.get('status'),
                winery.get('review_status'),
                json.dumps(winery.get('statistics')),
                winery.get('business_name'),
                winery.get('description'),
                winery.get('specialists_notes'),
                winery.get('phone'),
                winery.get('email'),
                winery.get('facebook'),
                winery.get('instagram'),
                winery.get('is_claimed'),
                winery.get('twitter'),
                winery.get('website'),
                winery.get('winemaker'),
                json.dumps(winery.get('address')),
                json.dumps(winery.get('location')),
                winery.get('region').get('id'),
            )

            self.cursor.execute(query, values)

        except AttributeError as e:
            print(f"An AttributeError occurred: {e}")

        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def import_wineries(self):
        self.load_wineries_ids()

        # tqdm is just for a little progress bar
        for winery_id in tqdm(self.wineries_ids):
            winery = self.fetch_winerie_by_id(winery_id)
            self.insert_winery(winery)

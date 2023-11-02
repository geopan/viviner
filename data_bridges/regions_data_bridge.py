import psycopg2
from data_bridges import DataBridge


class RegionsDataBridge(DataBridge):

    def __init__(self, conn):
        super().__init__(conn)
        self.regions = []

    def fetch_regions(self) -> list:
        res = self.r.get(f'regions')
        regions = res.json().get('regions', [])

        # Add filter for Australia
        self.regions = [region for region in regions if region.get(
            'country', {}).get('code') == 'au']

        return self.regions

    def insert_region(self, region):
        query = """
            INSERT INTO regions (id, name, seo_name, parent_id)
                VALUES (%s, %s, %s, %s)
            ON CONFLICT(id)
            DO UPDATE SET
                name = excluded.name,
                seo_name = excluded.seo_name,
                parent_id = excluded.parent_id;
            """

        try:

            values = (
                region.get('id'),
                region.get('name'),
                region.get('seo_name'),
                region.get('parent_id')
            )

            self.cursor.execute(query, values)

        except AttributeError as e:
            print(f"An AttributeError occurred: {e}")

        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def insert_regions(self, regions=None):

        if regions is None:
            regions = self.regions

        for r in regions:
            self.insert_region(r)

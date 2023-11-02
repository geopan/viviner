from data_bridges import DataBridge
import psycopg2

# https://api.vivino.com/grapes/1
# Explore deeper - I guess separate grape_detail table - only when has_detailed_info true


class GrapesDataBridge(DataBridge):

    def __init__(self, conn):
        super().__init__(conn)
        self.grapes = []

    def fetch_grapes(self) -> list:
        res = self.r.get(f'grapes')
        self.grapes = res.json().get('grapes', [])

        return self.grapes

    def insert_grape(self, grape):
        query = """
            INSERT INTO grapes (id, name, seo_name, has_detailed_info, parent_grape_id)
                VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(id)
            DO UPDATE SET
                name = excluded.name,
                seo_name = excluded.seo_name,
                has_detailed_info = excluded.has_detailed_info,
                parent_grape_id = excluded.parent_grape_id;
            """

        try:

            values = (
                grape.get('id'),
                grape.get('name'),
                grape.get('seo_name'),
                grape.get('has_detailed_info'),
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

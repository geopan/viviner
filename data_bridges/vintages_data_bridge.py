from data_bridges import DataBridge


class VintagesDataBridge(DataBridge):

    def fetch_vintages(self) -> list:
        res = self.r.get(f'vintages')
        return res.json().get('vintages', [])

    def fetch_vintage_by_id(self, vintage_id: str):
        res = self.r.get(f'vintages/{vintage_id}')
        return res.json().get('vintage', {})

    def insert_vintage(self, vintage):
        query = """
            INSERT INTO vintages (id, name, seo_name, has_detailed_info, parent_id)
                VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(id) ...
            """

        id = vintage.get('id')
        name = vintage.get('name')
        seo_name = vintage.get('seo_name')
        has_detailed_info = vintage.get('has_detailed_info')
        parent_id = vintage.get('parent_id')

        self.cursor.execute(
            query, (id, name, seo_name, has_detailed_info, parent_id))

    def insert_all(self):
        vintages = self.fetch()
        for g in vintages:
            self.insert_vintage(g)

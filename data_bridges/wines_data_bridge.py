from data_bridges import DataBridge

# https://api.vivino.com/wineries/254665/wines?per_page=50&sort=best_rated&include_all_vintages=true&language=en

# https://api.vivino.com/wines/${id}
# https://api.vivino.com/wines/${id}/tastes
# https://api.vivino.com/wines/${id}/reviews/_ranked
# https://api.vivino.com/vintages/${vintage.id}


class WinesDataBridge(DataBridge):

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
        return res.json().get('wine', {})

    def insert_wine(self, wine):
        query = """
            INSERT INTO wines (id, name, seo_name, has_detailed_info, parent_id)
                VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET
                name = excluded.name,
                seo_name = excluded.seo_name,
                has_detailed_info = excluded.has_detailed_info,
                region_id = excluded.region_id;

            """

        id = wine.get('id')
        name = wine.get('name')
        seo_name = wine.get('seo_name')
        has_detailed_info = wine.get('has_detailed_info')
        parent_id = wine.get('parent_id')

        self.cursor.execute(
            query, (id, name, seo_name, has_detailed_info, parent_id))

    def insert_all(self):
        wines = self.fetch()
        for g in wines:
            self.insert_wine(g)

import argparse

import utils.constants as c
from utils.requester import Requester


def get_arguments():
    """Gets arguments from the command line.

    Returns:
        A parser with the input arguments.

    """

    parser = argparse.ArgumentParser(usage='Scraps all wine URLs from Vivino.')

    parser.add_argument('output_file', help='Output .txt file', type=str)

    parser.add_argument(
        '-s', '--start_page', help='Starting page identifier', type=int, default=1)

    return parser.parse_args()


if __name__ == '__main__':
    # Gathers the input arguments and its variables
    args = get_arguments()
    output_file = args.output_file
    start_page = args.start_page

    # Instantiates a wrapper over the `requests` package
    r = Requester(c.BASE_URL)

    # Defines the payload, i.e., filters to be used on the search
    payload = {
        "language": "en",
        "country_codes[]": "au",
        # "food_ids[]": 20,
        # "grape_ids[]": 3,
        # "grape_filter": "varietal",
        # "min_rating": 3.7,
        # "order_by": "ratings_average",
        # "order": "desc",
        # "price_range_min": 25,
        # "price_range_max": 100,
        "region_ids[]": 475,
        # "wine_style_ids[]": 98,
        # "wine_type_ids[]": 1,
        # "wine_type_ids[]": 2,
        # "wine_type_ids[]": 3,
        # "wine_type_ids[]": 4,
        # "wine_type_ids[]": 7,
        # "wine_type_ids[]": 24,
    }

    # Performs an initial request to get the number of records (wines)
    res = r.get('explore/explore?', params=payload)

    print(res)

    n_matches = res.json()['explore_vintage']['records_matched']

    print(f'Number of matches: {n_matches}')

    # Opens the output file with append
    with open(output_file, 'r+') as f:

        wine_ids = []

        # Loop over each line in the file
        for line in f:
            # Strip leading/trailing whitespace and convert to integer
            id = int(line.strip())

            # Append the ID to the list
            wine_ids.append(id)

        # Iterates through the amount of possible pages
        for i in range(start_page, max(1, int(n_matches / c.RECORDS_PER_PAGE)) + 1):
            # Adds the page to the payload
            payload['page'] = i

            print(f'Scraping data from page: {payload["page"]}')

            # Performs the request and scraps the URLs
            res = r.get('explore/explore', params=payload)
            matches = res.json()['explore_vintage']['matches']

            # Iterates over every match
            for match in matches:

                wid = match["vintage"]["wine"]["id"]

                if wid in wine_ids:
                    print(f'{wid} found in array')
                    continue

                print(f'{wid} NOT found in array')

                wine_ids.insert(0, wid)

                # Dumps the URL to file
                f.write(f'{wid}\n')

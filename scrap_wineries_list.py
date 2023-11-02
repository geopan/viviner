import utils.constants as c
from utils.requester import Requester
from tqdm import tqdm


if __name__ == '__main__':

    # Instantiates a wrapper over the `requests` package
    r = Requester("https://api.vivino.com/")

    # Opens the output file with append
    with open('wine_id_list.txt', 'r') as f:

        wine_ids = []

        # Loop over each line in the file
        for line in f:
            # Strip leading/trailing whitespace and convert to integer
            id = int(line.strip())

            # Append the ID to the list
            wine_ids.append(id)

        with open('wineries_id_list.txt', 'a+') as winery_file:

            wineries_ids = []

            # Loop over each line in the file
            for line in winery_file:
                # Strip leading/trailing whitespace and convert to integer
                id = int(line.strip())

                # Append the ID to the list
                wineries_ids.append(id)

            # Iterates through the amount of possible pages
            for i in tqdm(wine_ids[2620:]):
                # for i in wine_ids:

                # Performs an initial request to get the number of records (wines)
                res = r.get(f'wines/{i}')

                winery = res.json()['winery']

                if winery['id'] in wineries_ids:
                    # print(f'{winery.get("name")} found')
                    continue

                # print(f'{winery.get("name")} NOT found in array')

                wineries_ids.insert(0, winery['id'])

                # Dumps the URL to file
                winery_file.write(f'{winery["id"]}\n')

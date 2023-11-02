import os
import argparse
import json
from dotenv import load_dotenv
import psycopg2 as pg

import data_bridges


load_dotenv()


def get_arguments():
    """
    Gets arguments from the command line.

    Returns:
        A parser with the input arguments.

    """

    parser = argparse.ArgumentParser(usage='Scraps wine data from Vivino.')

    parser.add_argument('-r', '--region_id', help='Region id', type=str)
    parser.add_argument('-g', '--grape_id', help='Grape id', type=str)
    parser.add_argument('-m', '--min_rating', default=3.8,
                        type=str, help='Wine minimum rating')
    parser.add_argument('--min_price', type=str, help='Min price')
    parser.add_argument('--max_price', type=str, help='Max price')
    parser.add_argument('-s', '--start_page',
                        help='Starting page identifier', type=int, default=1)
    parser.add_argument('-o', '--output_file',
                        default='output.json', help='Output .json file', type=str)
    parser.add_argument(
        '--import_grapes', help='Instruct program to also import grapes', action='store_true')
    parser.add_argument(
        '--import_regions', help='Instruct program to also import regions', action='store_true')
    parser.add_argument(
        '--import_wineries', help='Instruct program to also import wineries', action='store_true')
    parser.add_argument(
        '--import_wines', help='Instruct program to also import wines', action='store_true')

    return parser.parse_args()


if __name__ == '__main__':

    # Gathers the input arguments and its variables
    args = get_arguments()
    region_id = args.region_id
    grape_id = args.grape_id
    min_rating = args.min_rating
    min_price = args.min_price
    max_price = args.max_price
    output_file = args.output_file
    start_page = args.start_page

    # Connect to the PostgreSQL database
    conn = pg.connect(**{
        'dbname': os.getenv("VIVINER_DB_NAME", ),
        'user': os.getenv("VIVINER_DB_USER"),
        'password': os.getenv("VIVINER_DB_PASSWORD"),
        'host': os.getenv("VIVINER_DB_HOST"),
        'port': os.getenv("VIVINER_DB_PORT"),
    })

    # Create a cursor object
    # cur = conn.cursor()

    if args.import_grapes:
        grapes_bridge = data_bridges.GrapesDataBridge(conn)
        grapes_bridge.fetch_grapes()
        grapes_bridge.insert_grapes()
        grapes_bridge.commit()

        print(f'{len(grapes_bridge.grapes)} grapes imported in database')

    if args.import_regions:
        regions_bridge = data_bridges.RegionsDataBridge(conn)
        regions_bridge.fetch_regions()
        regions_bridge.insert_regions()
        regions_bridge.commit()

        print(f'{len(regions_bridge.regions)} regions imported in database')

    if args.import_wineries:
        wineries_bridge = data_bridges.WineriesDataBridge(conn)
        wineries_bridge.import_wineries()
        wineries_bridge.commit()

    if args.import_wines:
        wines_bridge = data_bridges.WinesDataBridge(conn)
        wines_bridge.import_wines()
        wines_bridge.commit()

    # print(len(grapes))
    # print(len(regions))

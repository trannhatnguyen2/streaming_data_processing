# import os
from time import sleep
import pandas as pd

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()


TABLE_NAME = "orders"
CSV_FILE = "./data/orders.csv"
NUM_ROWS = 1000


def main():

    pc = PostgresSQLClient(
        database="k6",
        user="k6",
        password="k6"
    )

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    # Loop over all columns and create random values
    df = pd.read_csv(CSV_FILE, nrows=NUM_ROWS)

    for _, row in df.iterrows():

        # Insert data
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(row)}
        """

        pc.execute_query(query)
        sleep(5)


if __name__ == "__main__":
    main()
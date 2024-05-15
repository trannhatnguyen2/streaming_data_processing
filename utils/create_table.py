import os

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()

def main():

    pc = PostgresSQLClient(
        database='k6',
        user='k6',
        password='k6',
    )

    # Create actors and movies tables
    ## MOVIES
    create_orders_table_query = """
        CREATE TABLE IF NOT EXISTS orders (
            order_date          DATE,
            order_time          TIME,
            order_number        VARCHAR(50),
            order_line_number   INT,
            customer_name       VARCHAR(100),
            product_name        VARCHAR(100),
            store               VARCHAR(100),
            promotion           VARCHAR(100),
            order_quantity      INT,
            unit_price          NUMERIC(12, 2),
            unit_cost           NUMERIC(12, 2),
            unit_discount       NUMERIC(12, 2),
            sales_amount        NUMERIC(12, 2)
        )
    """

    try: 
        pc.execute_query(create_orders_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")

if __name__ == "__main__":
    main()

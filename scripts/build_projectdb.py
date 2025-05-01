import os
from pprint import pprint

import psycopg2 as psql

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
    password = file.read().rstrip()

conn_string = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team26 dbname=team26_projectdb password={password}"

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.

    print("Creating tables...")
    with open(os.path.join("sql", "create_tables.sql")) as file:
        content = file.read()
        cur.execute(content)
    conn.commit()
    print("Tables created!")

    # Read the commands from the file and execute them.
    print("Importing data...")
    with open(os.path.join("sql", "import_data.sql")) as file:
        command = file.read()
        with open(os.path.join("data", "us_congestion_2016_2022/us_congestion_2016_2022.csv"), "r") as data:
            cur.copy_expert(command, data)

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()
    print("Imported data!")

    pprint(conn)
    cur = conn.cursor()

    print("Testing db...")
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql")) as file:
        commands = file.read().split(";")
        for command in commands:
            if command.strip():
                cur.execute(command)
                if cur.description:
                    result = cur.fetchall()
                    print(f"Results from command '{command.strip()}':")
                    pprint(result)

    print("Database setup completed successfully!")
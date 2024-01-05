import random
import datetime
import pandas as pd
import mysql.connector

# Replace these values with your MySQL server information
host = "localhost"
user = "root"
password = "root"
database = "encryptcon"


def startup():
    df = pd.read_csv("encryptcon/synthetic_financial_data.csv")

    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            query1 = """CREATE DATABASE IF NOT EXISTS encryptcon """
            cursor.execute(query1)
            query2 = """USE encryptcon"""
            cursor.execute(query2)

            # Create a table to store the data (if not exists)
            create_table_query1 = """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INT  AUTO_INCREMENT PRIMARY KEY,
                customer_id INT,
                merchant_id INT,
                amount DECIMAL(10, 2),
                transaction_time DATETIME,
                is_fraudulent INT,
                location VARCHAR(50),
                purchase_category VARCHAR(50)
            )
            """
            create_table_query2 = """
            CREATE TABLE IF NOT EXISTS account (
                customer_id INT,
                balance DECIMAL(10, 2)
            )
            """

            create_table_query3 = """
            CREATE TABLE IF NOT EXISTS blacklist (
                merchant_id INT primary key,
                merchant_name VARCHAR(50),
                merchant_location VARCHAR(50),
                merchant_contact VARCHAR(20)
            )
            """

            create_table_query4 = """
            CREATE TABLE IF NOT EXISTS whitelist (
                merchant_id INT primary key,
                merchant_name VARCHAR(50)
            )
            """
            cursor.execute(create_table_query1)
            cursor.execute(create_table_query2)
            cursor.execute(create_table_query3)
            cursor.execute(create_table_query4)

            # Copy data from DataFrame to MySQL table
            for index, row in df.iterrows():
                insert_query = f"""
                INSERT INTO transactions VALUES (
                    {row['transaction_id']},
                    {row['customer_id']},
                    {row['merchant_id']},
                    {row['amount']},
                    '{row['transaction_time']}',
                    {row['is_fraudulent']},
                    '{row['location']}',
                    '{row['purchase_category']}'
                )
                """
                cursor.execute(insert_query)

            # Insert data into the account table based on the conditions
            insert_account_query = """
                 INSERT INTO account (customer_id, balance)
                SELECT customer_id, SUM(amount) + %s AS balance
                FROM transactions
                GROUP BY customer_id
                """
            # Use a random number between 10000 and 25000 as the additional value
            random_number = random.randint(10000, 25000)
            cursor.execute(insert_account_query, (random_number,))

            # Commit the changes
            connection.commit()

            print("Data inserted successfully.")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def add_black_list(id, name, location, contact):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            insert_blacklist_query = """
                 INSERT INTO blacklist (merchant_id, merchant_name, merchant_location, merchant_contact)
                VALUES (%s, %s, %s, %s)
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(insert_blacklist_query,
                           (id, name, location, contact))

            # Commit the changes
            connection.commit()

            print("Data inserted successfully.")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def get_blacklist():
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            get_blacklist_query = """
                 SELECT * FROM blacklist
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(get_blacklist_query)
            result = cursor.fetchall()
            return result

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def add_whitelist(id, name):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            insert_whitelist_query = """
                 INSERT INTO whitelist (merchant_id, merchant_name)
                VALUES (%s, %s)
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(insert_whitelist_query,
                           (id, name))

            # Commit the changes
            connection.commit()

            print("Data inserted successfully.")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def get_whitelist():
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            get_whitelist_query = """
                 SELECT * FROM whitelist
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(get_whitelist_query)
            result = cursor.fetchall()
            return result

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def do_transaction(cust_id, merch_id, amount, status, local, category):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            insert_transaction_query = """
                 INSERT INTO transactions (customer_id, merchant_id, amount, transaction_time, is_fraudulent, location, purchase_category)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(insert_transaction_query, (cust_id, merch_id,
                           amount, datetime.datetime.now(), status, local, category))

            # Commit the changes
            connection.commit()

            print("Data inserted successfully.")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def get_balance(cust_id):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            get_balance_query = """
                 SELECT balance FROM account WHERE customer_id = %s
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(get_balance_query, (cust_id,))
            result = cursor.fetchone()
            return result[0]

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Initilization completed...")


def get_data_for_model(cust_id):
    try:
        # Establish a connection to the MySQL server
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL Server: {host}")
            print(f"Using database: {database}")

            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Insert data into the account table based on the conditions
            get_data_query = """
                 SELECT * FROM transactions WHERE customer_id = %s
                """
            # Use a random number between 10000 and 25000 as the additional value
            cursor.execute(get_data_query, (cust_id,))
            result = cursor.fetchall()
            auth, old_bal = predict(result)

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            if auth:
                query = """UPDATE accounts SET balance = %s WHERE customer_id = %s"""
                cursor.execute(query, (old_bal, cust_id))
                connection.commit()

            connection.close()


def predict(data):

    authFactor = 1

    if len(data.keys()) == 6:

        if data['merchant_id'] in [x[0] for x in get_whitelist()]:
            return 0

        if data['merchant_id'] in [y[0] for y in get_blacklist()]:
            return 1

        # Location Condition
        past_n_transactions = get_data_for_model(data['customer_id'])
        my_list = [x[6] for x in past_n_transactions]
        most_common_location = max(set(my_list), key=my_list.count)

        if data['location'] == most_common_location:
            authFactor -= 0.3

        elif data['location'] in my_list:
            authFactor -= 0.25

        else:
            authFactor += 0.3

    else:
        print("\n\n\t\tNOT SUFFICIENT INFORMATION")


startup()
add_black_list("3405", "Joe", "city-34", "xyz@mail.com")
add_black_list("5640", "John", "city-17", "abc@mail.com")
add_whitelist("777", "Hospital")
add_whitelist("456", "Clinic")

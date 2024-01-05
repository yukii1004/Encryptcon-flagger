import random
import datetime
import pandas as pd
import mysql.connector
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier

# Replace these values with your MySQL server information
host = "localhost"
user = "root"
password = "root"
database = "encryptcon"


def startup():
    df = pd.read_csv("synthetic_financial_data.csv")

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
            return result

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        # Close the connection, whether successful or not
        if 'connection' in locals() and connection.is_connected():
            connection.close()


def predict(data):

    authFactor = 0

    if len(data):

        balance = get_balance(data['customer_id'])

        if data['merchant_id'] in [x[0] for x in get_whitelist()]:
            return balance, 0

        if data['merchant_id'] in [y[0] for y in get_blacklist()]:
            return balance, 1

        # Location Condition
        past_n_transactions = get_data_for_model(data['customer_id'])
        my_list = [x[6] for x in past_n_transactions]
        most_common_location = max(set(my_list), key=my_list.count)

        if data['location'] == most_common_location:
            authFactor += 0.3

        elif data['location'] in my_list:
            authFactor += 0.25

        else:
            authFactor -= 0.3

        balance = get_balance(data['customer_id'])

        if float(balance)*0.95 < float(data['amount']):
            authFactor += 0.2

        # Model 1

        df = pd.read_csv("synthetic_financial_data.csv")

        df = df.drop(['transaction_id', 'transaction_time', 'card_type',
                      'customer_age', 'transaction_description'], axis=1)
        df = df.dropna()

        label_encoder = LabelEncoder()

        df['location'] = label_encoder.fit_transform(df['location'])
        df['purchase_category'] = label_encoder.fit_transform(
            df['purchase_category'])

        # Display the encoded DataFrame

        x = df.drop('is_fraudulent', axis=1)
        y = df['is_fraudulent']

        X_train, X_test, Y_train, Y_test = train_test_split(
            x, y, test_size=0.33, random_state=333)

        fraudulentclassifier1 = DecisionTreeClassifier(
            max_leaf_nodes=1000, random_state=0)

        fraudulentclassifier1.fit(X_train, Y_train)
        Y_predicted = fraudulentclassifier1.predict(X_test)
        # print("Accuracy Score Decision Tree : ", accuracy_score(Y_test, Y_predicted))

        # Model 2

        df = pd.read_csv("./synthetic_financial_data.csv")

        df = df.drop(['transaction_id', 'transaction_time', 'card_type',
                      'customer_age', 'transaction_description'], axis=1)
        df = df.dropna()

        label_encoder = LabelEncoder()

        df['location'] = label_encoder.fit_transform(df['location'])
        df['purchase_category'] = label_encoder.fit_transform(
            df['purchase_category'])

        # Display the encoded DataFrame

        x = df.drop('is_fraudulent', axis=1)
        y = df['is_fraudulent']

        X_train, X_test, Y_train, Y_test = train_test_split(
            x, y, test_size=0.33, random_state=333)

        fraudulentclassifier2 = KNeighborsClassifier(n_neighbors=3)

        fraudulentclassifier2.fit(X_train, Y_train)
        Y_predicted = fraudulentclassifier2.predict(X_test)
        # print("Accuracy Score KNN Classifier : ", accuracy_score(Y_test, Y_predicted))

        # MOdel 3

        df = pd.read_csv("./synthetic_financial_data.csv")

        df = df.drop(['transaction_id', 'transaction_time', 'card_type',
                      'customer_age', 'transaction_description'], axis=1)
        df = df.dropna()

        label_encoder = LabelEncoder()

        df['location'] = label_encoder.fit_transform(df['location'])
        df['purchase_category'] = label_encoder.fit_transform(
            df['purchase_category'])

        # Display the encoded DataFrame

        x = df.drop('is_fraudulent', axis=1)
        y = df['is_fraudulent']

        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            x, y, test_size=0.2, random_state=42)

        # Build a Random Forest Regression model
        fraudulentclassifier3 = RandomForestRegressor(
            n_estimators=100, random_state=42)

        # Train the model
        fraudulentclassifier3.fit(X_train, y_train)

        # Make predictions on the testing data
        y_pred = fraudulentclassifier3.predict(X_test)

        def preprocess_data(data):
            data = data.drop(['transaction_description', 'customer_age', 'transaction_id', 'transaction_time',
                              'card_type', 'is_fraudulent'], axis=1)

            label_encoder = LabelEncoder()
            data['location'] = label_encoder.fit_transform(data['location'])
            data['purchase_category'] = label_encoder.fit_transform(
                data['purchase_category'])

            return data

        # Preprocess the input data
        input_df = preprocess_data(pd.DataFrame(data, index=[0]))

        # Make predictions using the Decision Tree model
        Dec_Tree_prediction = fraudulentclassifier1.predict(input_df)
        print(f"Decision Tree Prediction: {Dec_Tree_prediction[0]}")

        # Make predictions using the K-Nearest Neighbors model
        knn_classifier_prediction = fraudulentclassifier2.predict(input_df)
        print(f"KNN Classifier Prediction: {knn_classifier_prediction[0]}")

        # Make predictions using the Random Forest model
        RF_prediction = fraudulentclassifier3.predict(input_df)
        print(f"RF Classifier Prediction: {RF_prediction[0]}")

        final_predictions = 0.6 * \
            Dec_Tree_prediction[0] + 0.15 * \
            knn_classifier_prediction[0] + 0.25*RF_prediction[0]
        print("final prediction: ", final_predictions)

        auth = (0.8 * final_predictions + authFactor*0.2)

        if auth >= 0.85:
            print("\n\n\t\tFLAG 3")
        if auth >= 0.4 and auth < 0.85:
            print("\n\n\t\tFLAG 2")

        if auth >= 0.3 and auth < 0.4:
            print("\n\n\t\tFLAG 1")
        if auth < 0.3:
            print("\n\n\t\tFLAG 0")

    else:
        print("\n\n\t\tNOT SUFFICIENT INFORMATION")


# startup()
# add_black_list("3405", "Joe", "city-34", "xyz@mail.com")
# add_black_list("5640", "John", "city-17", "abc@mail.com")
# add_whitelist("777", "Hospital")
# add_whitelist("456", "Clinic")
sample = {
    "transaction_id": "56",
    "customer_id": "1094",
    "merchant_id": "2015",
    "amount": "7964.55",
    "transaction_time": "2023-01-01 00:00:55",
    "is_fraudulent": "1",
    "card_type": "Discover",
    "location": "City-48",
    "purchase_category": "Groceries",
    "customer_age": "21",
    "transaction_description": "Purchase at Merchant-2015"
}
predict(sample)

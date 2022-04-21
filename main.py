# -------------------------------------------------------------------------------
# Name:        main.py
# Purpose:     Read DB data and upload it to S3 in a JSON file and vice versa
#
# Author:      Iván Miguel Molinero (Vermont-Solutions)
#
# Created:     13/04/2022
# Copyright:   (c)
# Licence:     <your licence>
# -------------------------------------------------------------------------------

import pymysql
import json
import re
import threading
import time
import signal
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import dotenv_values


class DataBase:
    def __init__(self):
        self.connection = pymysql.connect(
            host=sql_host, user=sqluser, password=sqlpassword, db=db_name  # ip
        )

        # Connect to DB
        self.cursor = self.connection.cursor()

        print("Conexión establecida con éxito.")

    def select_user(self, Apellido):
        sql = 'SELECT Nombre, Apellido, Posicion FROM tabla_prueba WHERE Apellido = "{}"'.format(
            Apellido
        )

        try:
            self.cursor.execute(sql)
            user = self.cursor.fetchone()

            print("Nombre:", user[0])
            print("Apellido:", user[1])
            print("Posicion:", user[2])

        except Exception as e:
            print(e)

    # Read data from a DB and write it in a JSOn file
    def data_to_json(self, tabla, json_file):
        global json_content

        try:
            while True:  # Infinite loop to read new data periodically
                # SQL commands
                sql = "SELECT * FROM {}".format(tabla)
                sql_col = "SHOW COLUMNS FROM {}".format(tabla)

                print("Dumping the data to the JSON file...")
                # List to dump in a JSON file
                list = []
                user_aux = {}
                i = 0  # User index
                self.cursor.execute(sql)  # Execute order in SQL. "SELECT * FROM table"
                data_db = self.cursor.fetchall()  # Extract data from DB table
                # Execute order in SQL. "SHOW COLUMNS FROM table"
                self.cursor.execute(sql_col)
                # Extract cols from DB table to traverse table
                cols = self.cursor.fetchall()
                for user in data_db:  # Loop for add data to list
                    for col in cols:
                        user_aux[col[0]] = user[i]  # Data[key] = valor
                        i += 1
                    list.append(user_aux)
                    user_aux = {}  # Reset variables
                    i = 0
                with open(json_file, "w") as outfile:  # Dump data in a JSON file
                    json.dump(list, outfile)
                json_content = list
                time.sleep(60)

                if exit_event.is_set():
                    break

        except Exception as e:
            print(e)

    def create_order(self, key, keys, last_object, object):
        str_to_add = '"' + str(object[key]) + '"'  # Start order: "value"
        if key == keys[-1]:  # If it the last col...
            if last_object:
                # If it is the last object to add, add the value and then close the order with ");"
                self.sql_insert += str_to_add + ");"
            else:
                # If not, add the value and then close the object and star one new with "),\n("
                self.sql_insert += str_to_add + "),\n("
        else:
            self.sql_insert += str_to_add + ", "  # If not, add the value and ", "

    # Read the JSON file content and upload it to DB
    def json_to_db(self, s3_table, s3_file):
        # SQL commands
        sql = "CREATE TABLE {} (".format(s3_table)
        self.sql_insert = "INSERT INTO {} VALUES\n(".format(s3_table)
        last_object = False
        characters = "'[]"  # Characters to delete when read the keys

        try:
            with open(s3_file, "r") as data_file:  # Read the JSON file content
                data = json.load(data_file)
                keys = list(data[0].keys())  # Create a list with the dict's keys
            for key in keys:
                # Read the class type (int, string, float...) of the data
                class_value_aux = str(type(data[0][key]))
                # Delete chars that is not class value: <class 'str'> => str
                class_value = (re.findall(r"'(.*?)'", class_value_aux))[0]
                if class_value == "str":  # If class value is str...
                    # Switch for "char" because str is char in MySQL
                    class_value = "char(50)"
                if key == keys[-1]:
                    # If it is the last key: add the key, its class value and close the command with ")"
                    sql += key + " " + class_value + ")"
                else:
                    # If not:  add the key, its class value and ", " to add the next key
                    sql += key + " " + class_value + ", "
            # Execute the order in SQL: "CREATE TABLE table (..."
            self.cursor.execute(sql)
            print("Table", s3_table, "created!")
            for object in data:  # Loop to create the SQL insert order
                if object == data[-1]:
                    last_object = True
                for key in keys:
                    # Create the SQL insert order
                    self.create_order(key, keys, last_object, object)
            self.cursor.execute(
                self.sql_insert
            )  # Execute the order in SQL: "INSERT INTO table VALUES\n("
            self.connection.commit()
            print(self.cursor.rowcount, "values inserted in DB!")

        except Exception as e:
            # If the table already exists in DB => update it
            if str(e).startswith("(1050,"):
                print(s3_table, "already exists. Proceeding to update it")
                for object in data:
                    index_object = 0
                    mismo_objeto = False
                    for key in keys:
                        print(mismo_objeto)
                        if not mismo_objeto:  # If it is not the same object...
                            if key == keys[0]:
                                keys_string = "".join(
                                    x for x in str(keys) if x not in characters
                                )  # Create a string of the keys deleting characters => '[]
                                sql_object = (
                                    "SELECT "
                                    + keys_string
                                    + " FROM "
                                    + s3_table
                                    + " WHERE "
                                    + key
                                    + ' = "'
                                    + str(object[key])
                                    + '"'
                                )  # SQL command to read an object
                                self.cursor.execute(
                                    sql_object
                                )  # Execute the order in SQL: "SELECT (all the values) from an object in the table"
                                print("ORDEN:", sql_object)
                                # Save the SQL object in a variable
                                all_object = self.cursor.fetchone()
                                print("ALL OBJECT:", all_object)
                                if str(all_object) != "None":
                                    # If the objects in JSON and in DB are the same...
                                    print("MISMO OBJETO")
                                    mismo_objeto = True
                                    value_sql = all_object[index_object]
                                    print("SQL:", value_sql)
                                    print("OBJECT:", object[key])
                                    if value_sql != object[key]:
                                        # If the value has changed...
                                        sql_update = "UPDATE {} SET {} = '{}' WHERE {} = '{}'".format(
                                            s3_table,
                                            key,
                                            object[key],
                                            keys[0],
                                            all_object[0],
                                        )  # SQL command to update an object
                                        print("ORDEN:", sql_update)
                                        # Execute the order in SQL: "UPDATE {} SET {} = '{}' WHERE {} = '{}'"
                                        self.cursor.execute(sql_update)
                                    index_object += 1
                                else:  # If they are not the same... => Create order to add the new object
                                    print("¡¡¡AAAAAAH OBJETO NUEVO!!!!")
                                    mismo_objeto = False
                                    self.create_order(key, keys, last_object, object)
                        # If the same object...
                        else:
                            print("¡¡¡AAAAAAH NOOOOOO OBJETO NUEVO!!!!")
                            print("")
                            value_sql = all_object[index_object]
                            if value_sql != object[key]:
                                # If the value has changed...
                                print("Procedemos a actualizar...")
                                sql_update = (
                                    "UPDATE {} SET {} = '{}' WHERE {} = '{}'".format(
                                        s3_table,
                                        key,
                                        object[key],
                                        keys[0],
                                        all_object[0],
                                    )
                                )  # SQL command to update an object
                                print("ORDEN:", sql_update)
                                # Execute the order in SQL: "UPDATE {} SET {} = '{}' WHERE {} = '{}'"
                                self.cursor.execute(sql_update)
                                self.connection.commit()
                            index_object += 1
                self.sql_insert = self.sql_insert[:-3]
                # Execute the order in SQL: "INSERT INTO table VALUES\n("
                print("ORDEN FINAL A EJECUTAR:", self.sql_insert)
                self.cursor.execute(self.sql_insert)
                self.connection.commit()
                print(self.cursor.rowcount, "values inserted in DB!")

            else:
                print(e)


def signal_handler(signum, frame):
    exit_event.set()


class S3_aws:
    def __init__(self):
        self.s3 = boto3.client(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )

    def upload_to_aws(self, local_file, bucket, s3_file):

        try:
            self.s3.upload_file(local_file, bucket, s3_file)
            print("Upload successful!")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def download_from_aws(self, bucket, s3_file, local_file):
        try:
            self.s3.download_file(bucket, s3_file, local_file)
            print("Download successful!")
        except Exception as e:
            print("Download error!", e)


if __name__ == "__main__":
    # Get variables from config file (you should call it credentials.env)
    config = dotenv_values("./credentials.env")
    ACCESS_KEY = config["aws_access_key_id"]
    SECRET_KEY = config["aws_secret_access_key"]
    db_name = config["bd"]
    sqlpassword = config["sqlpassword"]
    sqluser = config["sqluser"]
    sql_host = config["sql_host"]
    table_db = config["table_db"]
    json_file = config["json_file"]
    bucket_s3 = config["bucket_s3"]
    s3_json_file = config["s3_json_file"]
    file_name_s3_to_local = config["file_name_s3_to_local"]
    s3_table = config["s3_table"]
    json_content = []

    exit_event = threading.Event()

    # Load database
    database = DataBase()

    # Consult DB for new data
    signal.signal(signal.SIGINT, signal_handler)
    consult_db = threading.Thread(
        target=database.data_to_json, args=(table_db, json_file)  # Dump data to JSON
    )
    consult_db.start()

    # Load s3 bucket
    s3 = S3_aws()

    # Send POST to API Gateway with JSON file
    response = requests.post(
        "https://0rnq7qewhh.execute-api.eu-west-3.amazonaws.com/test/jsontos3",
        headers={"Content-Type": "application/json"},
        json=json_content,
    )
    print(response.text)

    # Send GET to API Gateway
    response_get = requests.get(
        "https://0rnq7qewhh.execute-api.eu-west-3.amazonaws.com/test/jsontos3",
    )
    print(response_get.text)

    # Load JSON content
    data = json.loads(response_get.text)
    # Write JSON content in a file
    with open(file_name_s3_to_local, "w") as outfile:
        json.dump(data, outfile)

    # Create table in DB for S3 file
    database.json_to_db(s3_table, file_name_s3_to_local)

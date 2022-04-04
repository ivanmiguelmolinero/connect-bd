import pymysql
import json
import re
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import dotenv_values


class DataBase:
    def __init__(self):
        self.connection = pymysql.connect(
            host=sql_host, user=sqluser, password=sqlpassword, db=db_name  # ip
        )

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

    """
    def select_user(self, tabla):
        sql = 'SELECT * FROM {}'.format(tabla)

        try:
            self.cursor.execute(sql)
            user = self.cursor.fetchall()

            print("Nombre:", user[0][0], " Apellido:", user[0][1], " Posicion:", user[0][2])
            print("Nombre:", user[1][0], " Apellido:", user[1][1], " Posicion:", user[1][2])
            print("Nombre:", user[2][0], " Apellido:", user[2][1], " Posicion:", user[2][2])

        except Exception as e:
            raise
    """

    """
    def select_user(self, id):
        sql = 'SELECT id, username, emai FROM users WHERE id = {}'.format(id)

        try:
            self.cursor.execute(sql)
            user = self.cursor.fetchone()

            print("Nombre:", user[0])
            print("Apellido:", user[1])
            print("Posicion:", user[2])

        except Exception as e:
            raise
    """

    def data_to_json(self, tabla):
        sql = "SELECT * FROM {}".format(tabla)
        sql_col = "SHOW COLUMNS FROM {}".format(tabla)

        try:
            print("Volcando los datos al archivo JSON...")
            list = []
            user_aux = {}
            i = 0
            self.cursor.execute(sql)
            data_db = self.cursor.fetchall()
            self.cursor.execute(sql_col)
            cols = self.cursor.fetchall()
            for user in data_db:
                for col in cols:
                    user_aux[col[0]] = user[i]
                    i += 1
                list.append(user_aux)
                user_aux = {}
                i = 0
            with open(json_file, "w") as outfile:
                json.dump(list, outfile)

        except Exception as e:
            print(e)

    def json_to_db(self, tabla_s3, s3_file):
        sql = "CREATE TABLE {} (Producto char(20) primary key, Cantidad int, Precio float, Prioridad char(20), Destinatario char(20))".format(
            tabla_s3
        )
        str_prueba = "CREATE TABLE prueba_s3 ("
        sql_insert = "INSERT INTO prueba_s3 VALUES\n("
        last_object = False

        try:
            try:
                self.cursor.execute(sql)
                print("Table created!")
            except Exception as e:
                print("ERROR:", e)
            with open(s3_file, "r") as data_file:
                data = json.load(data_file)
                print(type(data))
                print(data[0]["Precio"])
                keys = list(data[0].keys())
            for key in keys:
                class_value_aux = str(type(data[0][key]))
                # print(class_value_aux)
                class_value = (re.findall(r"'(.*?)'", class_value_aux))[0]
                if class_value == "str":
                    class_value = "char(50)"
                # print(class_value)
                if key == keys[-1]:
                    print("ÚLTIMO")
                    str_prueba += key + " " + class_value + ")"
                else:
                    str_prueba += key + " " + class_value + ", "
            print(str_prueba)
            # self.cursor.execute(str_prueba)
            print("Table prueba_s3 created!")
            for object in data:
                if object == data[-1]:
                    last_object = True
                for key in keys:
                    str_to_add = '"' + str(object[key]) + '"'
                    if key == keys[-1]:
                        print("ÚLTIMO VALOR")
                        if last_object:
                            print("ULTIMO VALOR DEL ÚLTIMO OBJETO")
                            sql_insert += str_to_add + ");"
                        else:
                            sql_insert += str_to_add + "),\n("
                    else:
                        sql_insert += str_to_add + ", "
            print(sql_insert)
            self.cursor.execute(sql_insert)
            print("Values inserted in DB!")

        except Exception as e:
            print(e)


class S3_aws:
    def __init__(self):
        self.s3 = boto3.client(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )

    def upload_to_aws(self, local_file, bucket, s3_file):
        # s3 = boto3.client('s3', aws_access_key_id = ACCESS_KEY,
        #                   aws_secret_access_key = SECRET_KEY)

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

    # Load database
    database = DataBase()
    database.select_user("Varios")
    # Dump data to JSON
    database.data_to_json(table_db)

    # Load s3 bucket
    s3 = S3_aws()

    # Upload JSON file to bucket in AWS
    s3.upload_to_aws(json_file, bucket_s3, s3_json_file)

    # Download JSON file from AWS
    s3.download_from_aws(bucket_s3, s3_json_file, file_name_s3_to_local)

    # Create table in DB for S3 file
    database.json_to_db("tabla_s3", file_name_s3_to_local)

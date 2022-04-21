import subprocess
import boto3
from tkinter import Tk, Frame, Label, Button
from dotenv import dotenv_values


class S3_aws:
    def __init__(self):
        self.s3 = boto3.client(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )

    def download_from_aws(self, bucket, s3_file, local_file):
        try:
            print("Downloading main.py...")
            self.s3.download_file(bucket, s3_file, local_file)
            print("Download successful!")
        except Exception as e:
            print("Download error!", e)


def cmd(str_in):  # ,commando
    if str_in == "Instalar":
        print("Pos instalamos")
        subprocess.run("pyinstaller --windowed --onefile ./main.py", shell=True)
    else:
        print("Comando desconocido, se imprime salida por defecto")
        subprocess.run("clear", shell=True)


def codigoBoton():
    s3.download_from_aws(bucket_s3, "main.py", "main.py")

    # print("Introduzca comando: ", end="")
    # str_in = input()
    cmd("Instalar")


if __name__ == "__main__":
    # Get variables from config file (you should call it credentials.env)
    config = dotenv_values("./credentials.env")
    ACCESS_KEY = config["aws_access_key_id"]
    SECRET_KEY = config["aws_secret_access_key"]
    bucket_s3 = config["bucket_s3"]

    # Load s3 bucket
    s3 = S3_aws()

    # GUI
    raiz = Tk()

    raiz.title("Installer")

    raiz.resizable(True, True)

    raiz.iconbitmap("installer.ico")

    # raiz.config(bg="grey")

    raiz.geometry("640x320")

    miFrame = Frame(raiz, width=500, height=400)
    miFrame.pack()

    miLabel = Label(miFrame, text="Presiona 'Instalar' para instalar el programa.")
    miLabel.pack()

    botonInstall = Button(raiz, text="Instalar", command=codigoBoton)
    botonInstall.pack()

    raiz.mainloop()

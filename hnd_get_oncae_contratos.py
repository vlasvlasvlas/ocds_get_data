import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from zipfile import ZipFile
import io

# bcpandas FAST SQL INSERTS. FIXED LATIN1: https://github.com/vlasvlasvlas/bcpandas
from bcpandas import SqlCreds, to_sql
import gc
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
import os

project_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(project_folder, ".env"))

# to_sql, .env needed
creds = SqlCreds(
    os.getenv("TO_SQL_HOST"),
    os.getenv("TO_SQL_DBSTAGE"),
    os.getenv("TO_SQL_USER"),
    os.getenv("TO_SQL_PWD"),
)

# opensource endpoints per year:
# honducompras, catalogo electronico, difusiondirecta

listaJson2019 = [
    {
        "nombre": "HonduCompras",
        "anio": "2019",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/HC1/HC1_datos_2019.zip",
        "desc": "compras realizadas por los procesos de contratación tradicionales como ser compras menores y licitaciones y la información de entidades compradoras y proveedores",
    },
    {
        "nombre": "CatalogoElectronico",
        "anio": "2019",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/CE/CE_datos_2019.zip",
        "desc": "contratos publicados en el sistema de información Registro de Contratos y Garantías",
    },
    {
        "nombre": "DifusionDirecta",
        "anio": "2019",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/DDC/DDC_datos_2019.zip",
        "desc": "contratos publicados en Difusión Directa de Contratos",
    },
]


listaJson2020 = [
    {
        "nombre": "HonduCompras",
        "anio": "2020",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/HC1/HC1_datos_2020.zip",
        "desc": "compras realizadas por los procesos de contratación tradicionales como ser compras menores y licitaciones y la información de entidades compradoras y proveedores",
    },
    {
        "nombre": "CatalogoElectronico",
        "anio": "2020",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/CE/CE_datos_2020.zip",
        "desc": "contratos publicados en el sistema de información Registro de Contratos y Garantías",
    },
]


listaJson2021 = [
    {
        "nombre": "HonduCompras",
        "anio": "2021",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/HC1/HC1_datos_2021.zip",
        "desc": "compras realizadas por los procesos de contratación tradicionales como ser compras menores y licitaciones y la información de entidades compradoras y proveedores",
    },
    {
        "nombre": "CatalogoElectronico",
        "anio": "2021",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/CE/CE_datos_2021.zip",
        "desc": "contratos publicados en el sistema de información Registro de Contratos y Garantías",
    },
]


listaJson2022 = [
    {
        "nombre": "HonduCompras",
        "anio": "2022",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/HC1/HC1_datos_2022.zip",
        "desc": "compras realizadas por los procesos de contratación tradicionales como ser compras menores y licitaciones y la información de entidades compradoras y proveedores",
    },
    {
        "nombre": "CatalogoElectronico",
        "anio": "2022",
        "url": "https://datosabiertos.oncae.gob.hn/datosabiertos/CE/CE_datos_2022.zip",
        "desc": "contratos publicados en el sistema de información Registro de Contratos y Garantías",
    },
]


# just test 2022
for origen in listaJson2022:

    url = origen["url"]
    nombre = origen["nombre"]
    anio = origen["anio"]

    response = requests.get(url, stream=True, timeout=60, verify=False)

    with ZipFile(io.BytesIO(response.content)) as zip_file:

        dfs = {
            text_file.filename: pd.read_csv(
                zip_file.open(text_file.filename), quotechar='"'
            )
            for text_file in zip_file.infolist()
            if text_file.filename.endswith(".csv")
        }

        for df in dfs.keys():

            # get df

            try:
                print("table name:")
                tablename = (
                    "tmp_oncae_source"
                    + str(df).replace("/", "_").replace(".csv", "").lower()
                )

                print(tablename)
                # clean
                dfs[df].columns = (
                    dfs[df]
                    .columns.str.strip()
                    .str.lower()
                    .str.replace(" ", "_")
                    .str.replace("(", "")
                    .str.replace(")", "")
                    .str.replace("/", "_")
                )

                # delimiter chars
                dfs[df] = (
                    dfs[df]
                    .replace("\\t", "", regex=True)
                    .replace("|", "", regex=True)
                    .replace(";", "", regex=True)
                )

                # quote chars
                dfs[df] = (
                    dfs[df].replace("#", "", regex=True).replace("~", "", regex=True)
                )

                print(dfs[df])
                to_sql(dfs[df], tablename, creds, index=False, if_exists="replace")
                gc.collect()  # memory garbage collect

            except SystemExit:
                print("--> systemexit except: {}".format(datetime.now()))

            except:
                print("--> insert except")
                print(sys.exc_info())

from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, text
from dataclasses import dataclass
from selenium import webdriver
from datetime import datetime
import pandas as pd
import requests
import warnings
import platform
import shutil
import time
import os


# Clase para el resultado del bloque
@dataclass
class Result:
    result: bool
    error: str | None = None


# Funciones auxiliares
def get_paths() -> tuple[str, str]:
    # Definimos la ruta principal
    folder_path = os.getcwd()
    # Definimos la ruta de las descargas
    downloads_path = os.path.join(folder_path, 'downloads')
    # Definimos la ruta del geckodriver
    geckodriver_path = GeckoDriverManager().install()
    # Terminamos la función regresando los paths
    return geckodriver_path, downloads_path


def recreate_folder(path: str) -> None:
    # Comprobamos si existe la carpeta
    if os.path.exists(path):
        # Eliminamos la carpeta
        shutil.rmtree(path)
    # Creamos la carpeta
    os.mkdir(path)
    # Terminamos la función
    return


def get_activate_delegations(db_user: str, db_user_password: str, db_host: str, db_port: str,
                             db_name: str) -> tuple[dict[int, str], list[str]]:
    # Cadena de conexión
    connection_string = f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}"
    # Creamos el engine
    engine = create_engine(connection_string)
    # Creamos la cadena de petición
    query = text("SELECT delegation_id, delegation_name FROM delegations WHERE is_active = 1")
    # Creamos el diccionario y la lista final
    delegations_dict = {}
    delegation_list = []
    # Creamos la conexión
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    # Creamos el diccionario y la lista con el resultado
    for delegation_id, delegation_name in result:
        delegations_dict[delegation_id] = delegation_name
        delegation_list.append(delegation_name)
    # Terminamos la función regresando el diccionario y la lista de nombres
    return delegations_dict, delegation_list


# Función main
def main_contracts(db_user: str, db_user_password: str, db_host: str, db_port: str,
                   db_name: str) -> Result:
    print("\t[Contracts Block] Scraping & Processing 📝")
    # Obtenemos los paths a usar
    try:
        geckodriver_path, downloads_path = get_paths()
        print(f"\t • Block paths:\n"
              f"\t\t🦎 geckodriver: {geckodriver_path}\n"
              f"\t\t📥 downloads: {downloads_path}")
    except Exception as e:
        print("\t ❌ Failed to retrieve block paths")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Recreamos la carpeta de descargas
    try:
        recreate_folder(downloads_path)
        print(f"\t • Downloads folder recreated successfully")
    except Exception as e:
        print(f"\t ❌ Failed to recreate downloads folder")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Obtenemos la lista de delegaciones activas y su diccionario
    try:
        delegations_dict, delegation_list = get_activate_delegations(db_user, db_user_password, db_host, db_port,
                                                                     db_name)
        print(f"\t • Delegations loaded successfully ({len(delegation_list)})")
    except Exception as e:
        print(f"\t ❌ Failed to get delegation list")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función regresando el resultado
    return Result(result=True)
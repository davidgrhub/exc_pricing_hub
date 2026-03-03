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
import platform
import warnings
import shutil
import time
import os


# Clase para el resultado del bloque
@dataclass
class Result:
    result: bool
    error: str | None = None


# Funciones auxiliares
def get_unique_product_ids(db_user: str, db_user_password: str, db_host: str, db_port: int,
                           db_name: str) -> list[int]:
    # Cadena de conexión
    connection_string = f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}"
    # Creamos el engine
    engine = create_engine(connection_string)
    # Query para obtener IDs únicos de la tabla final_strategies
    query = text("SELECT DISTINCT product_id FROM final_strategies WHERE product_id IS NOT NULL")
    # Lista de ids
    product_ids = []
    # Ejecutamos la conexión y extraemos los datos
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    # Convertimos el resultado
    product_ids = [row[0] for row in result]
    # Terminamos la función regresando la lista
    return product_ids


# Función main
def main_images(db_user: str, db_user_password: str, db_host: str, db_port: int, db_name: str,
                   headless: bool, timeout: int, user_mail: str, user_password: str, max_workers: int) -> Result:
    print("\t[Images Block] Scraping & Extracting Links 🖼️")
    # Obtenemos la lista de ids unicos para buscar sus imagenes
    try:
        unique_ids = get_unique_product_ids(db_user, db_user_password, db_host, db_port, db_name)
        print(f"\t • Products list loaded successfully ({len(unique_ids)})")
    except Exception as e:
        print(f"\t ❌ Failed to get products list")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función regresando el resultado
    return Result(result=True)
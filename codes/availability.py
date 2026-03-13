from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dataclasses import dataclass
from selenium import webdriver
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
                           db_name: str) -> list[dict]:
    # Cadena de conexión
    connection_string = f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}"
    # Creamos el engine
    engine = create_engine(connection_string)
    # Query para obtener IDs únicos de la tabla final_strategies
    query = text("""
        SELECT DISTINCT product_id, delegation_id 
        FROM final_strategies 
        WHERE product_id IS NOT NULL AND delegation_id IS NOT NULL
    """)
    # Ejecutamos la conexión y extraemos los datos
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    # Convertimos el resultado
    pairs = [{"product_id": row[0], "delegation_id": row[1]} for row in result]
    # Terminamos la función regresando la lista
    return pairs


# Funciones para scraping
def get_driver(geckodriver_path: str, headless: bool, timeout: int) -> tuple[webdriver, WebDriverWait]:
    # Declaramos si el sistema operativo es windows
    is_windows = platform.system() == "Windows"
    # Declaramos el servicio del driver
    service = Service(geckodriver_path)
    # Configuramos las opciones
    options = webdriver.FirefoxOptions()
    options.set_preference("intl.accept_languages", "en-US,en")
    options.add_argument("-private")
    firefox_exe = "/usr/bin/firefox-esr" if not is_windows else r"C:\Program Files\Mozilla Firefox\firefox.exe"
    options.binary_location = firefox_exe
    # Configuramos el headless
    if not (is_windows and headless is False):
        options.add_argument("--headless")
    # Iniciamos el driver
    driver = webdriver.Firefox(options=options, service=service)
    # Ingresamos el tiempo de espera
    wait = WebDriverWait(driver, timeout)
    # Terminamos la función regresando driver y wait
    return driver, wait


def run_scraping(element: dict, geckodriver_path: str, timeout: int, headless: bool) -> dict:
    # Inicializamos las variables
    data = {"id": element['product_id'], "availability": "", "start_date": "", "end_date": "", "deeplink": ""}
    # Iniciamos el driver
    driver, wait = get_driver(geckodriver_path, headless, timeout)
    # Declaramos el link
    link = (f"https://www.nexustours.com/services/details.aspx?serviceClassFilter=MatchAll&prov=SGN&idioma=en&"
            f"destinationID={element['delegation_id']}&paxs=20&accion=searchservices&nationality=US&"
            f"productType=TKT&productID={element['product_id']}&nights=8&addDays=2")
    # Manejo de error para cerrar el driver
    try:
        # Ingresamos el link del producto
        driver.get(link)
        # Comprobamos disponibilidad
        wait.until(ec.visibility_of_element_located((By.XPATH, "//button[text()='Options']")))
        # Si tenemos disponibilidad agregamos a la data
        data["availability"] = 1
    except TimeoutException:
        # Si tenemos error
        print(f"\t\t❌ Failed to get availability from ID {element['product_id']}")
        data["availability"] = 0
    finally:
        # Salimos del scraping
        driver.close()
        driver.quit()
        # Agregamos los valores faltantes
        data["start_date"] = (datetime.now() + timedelta(2)).strftime("%d-%m-%Y")
        data["end_date"] = (datetime.now() + timedelta(10)).strftime("%d-%m-%Y")
        data["deeplink"] = link
    # Terminamos la función regresando la data
    return data


def scraping(unique_product: list[dict], geckodriver_path: str, timeout: int, headless: bool,
             max_workers: int) -> pd.DataFrame:
    # Creamos la lista final
    results_list = []
    # Paralelismo para scraping
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_scraping, element, geckodriver_path, timeout, headless): element
            for element in unique_product
        }
        for f in as_completed(futures):
            # Recuperamos el id del tour
            item_id = futures[f]
            try:
                scraped_data = f.result()
                if scraped_data:
                    # Guardamos para el DataFrame
                    results_list.append(scraped_data)
            except Exception as e:
                print(f"\t\t❌ Worker failed for ID {item_id}: {e}")
    # Al terminar el bucle, creamos el DataFrame
    df_results = pd.DataFrame(results_list)
    # Terminamos la función regresando el dataframe
    return df_results


def upload_data(df: pd.DataFrame, db_user: str, db_user_password: str, db_host: str, db_port: int,
                db_name: str) -> None:
    # Creamos la conexión
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}")
    # Agregamos el dataframe a la base de datos
    df.to_sql('availability', con=engine, if_exists='replace', index=False)
    # Terminamos la función
    return


# Función main
def main_availability(db_user: str, db_user_password: str, db_host: str, db_port: int, db_name: str,
                      headless: bool, timeout: int, max_workers: int) -> Result:
    print("\t[Availability Block] Scraping & Deeplinks 🔍")
    # Obtenemos la lista de ids unicos para buscar sus imagenes
    try:
        unique_product = get_unique_product_ids(db_user, db_user_password, db_host, db_port, db_name)
        print(f"\t • Products list loaded successfully ({len(unique_product)})")
    except Exception as e:
        print(f"\t ❌ Failed to get products list")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Obtenemos la disponibilidad por producto
    try:
        print("\t • Starting information scraping")
        geckodriver_path = GeckoDriverManager().install()
        df = scraping(unique_product, geckodriver_path, timeout, headless, max_workers)
        print(df)
    except Exception as e:
        print("\t ❌ Failed to perform scraping for availability")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    try:
        print("\t • Uploading tour availability to database")
        upload_data(df, db_user, db_user_password, db_host, db_port, db_name)
        print("\t\tData uploaded successfully")
    except Exception as e:
        print("\t ❌ Failed to upload data to database")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función regresando el resultado
    return Result(result=True)
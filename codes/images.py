from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common import TimeoutException
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


def sing_in(driver: webdriver, wait: WebDriverWait, user: str, user_password: str) -> None:
    # Ingresamos a la Intranet
    driver.get('https://www.nexustours.com/intranet/login.aspx')
    # Login con azure
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//a[@id="_ctl0_data_holder_LoginAzure"]'))).click()
    # Ingresamos el correo
    mail = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="email"]')))
    mail.click()
    mail.send_keys(user)
    time.sleep(1.2)
    # Submit
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="submit"]'))).click()
    time.sleep(2)
    # Ingresamos la contraseña
    password = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="password"]')))
    password.click()
    password.send_keys(user_password)
    time.sleep(1.2)
    # Submit
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="submit"]'))).click()
    # Mantenemos la sesión
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="submit"]'))).click()
    # Esperamos a que entre a la intranet
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@id="_ctl0_toolbar_holder_localizador_rapido"]')))
    # Terminamos la función
    return


def get_url_image(wait: WebDriverWait, driver: webdriver) -> str:
    # Esperamos a que el iframe esté disponible
    wait.until(ec.frame_to_be_available_and_switch_to_it((By.ID, "iframeGeneral")))
    # Buscamos el apartado de imagenes
    images_div = wait.until(ec.visibility_of_element_located(
        (By.ID, 'panelImagenes_header-targetEl')))
    click_button = images_div.find_element(By.XPATH, './div[2]/img')
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", click_button)
    driver.execute_script("arguments[0].click();", click_button)
    # Esperar al contenedor de la lista de imagenes
    container = wait.until(ec.visibility_of_element_located(
        (By.ID, 'listadoImagenes')))
    # Obtenemos los elementos de la lista
    cells = container.find_elements(By.XPATH, ".//table/tbody/tr/td")
    # Declaramos las variables a usar en el for
    best_url = ""
    min_ranking = float('inf')
    # Pasamos por cada uno de los elementos
    for cell in cells:
        try:
            ranking_input = cell.find_element(By.XPATH, ".//input[@type='text']")
            current_ranking = int(ranking_input.get_attribute("value"))
            img_element = cell.find_element(By.XPATH, ".//div[2]/img")
            img_url = img_element.get_attribute("src")
            # Si es 0, terminamos y regresamos YA.
            if current_ranking == 0:
                return img_url
            # Si no es 0, verificamos si es el menor que hemos visto hasta ahora
            if current_ranking < min_ranking:
                min_ranking = current_ranking
                best_url = img_url
        except Exception:
            continue
    # Terminamos la función regresando el src
    return best_url


def run_scraping(product_id: int, geckodriver_path: str, timeout: int, headless: bool, user: str,
                 user_password: str) -> str:
    # Iniciamos el driver
    driver, wait = get_driver(geckodriver_path, headless, timeout)
    # Manejo de error para cerrar el driver
    try:
        # Ingresamos a intranet
        sing_in(driver, wait, user, user_password)
        # Ingresamos a la configuración del tour
        driver.get(f'https://www.nexustours.com/Intranet/serviciosV2/TabMantenimiento.aspx?servicio={product_id}')
        # Obtenemos la imagen del tour
        url = get_url_image(wait, driver)
    except TimeoutException:
        # Si tenemos error el link sera null
        url = ""
    finally:
        # Salimos del scraping
        driver.close()
        driver.quit()
    # Terminamos la funcion regresando el link
    return url


def clean_url(raw_url) -> str:
    try:
        # Buscamos la parte que empieza después de '&img='
        path = raw_url.split('&img=')[1]
        # Cortamos en el siguiente '&'
        clean_path = path.split('&')[0]
        # Retornamos la URL construida
        return f"https://www.nexustours.com/images/upload{clean_path}"
    except (IndexError, Exception):
        # Si no existe '&img=' o ocurre un error, regresamos vacío
        return ""


def scraping(unique_ids: list[int], geckodriver_path: str, timeout: int, headless: bool,
             user_mail: str, user_password: str, max_workers: int) -> pd.DataFrame:
    # Creamos la lista final
    results_list = []
    # Paralelismo para scraping
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_scraping, id_tour, geckodriver_path, timeout, headless, user_mail,
                            user_password): id_tour
            for id_tour in unique_ids
        }
        for f in as_completed(futures):
            # Recuperamos el id del tour
            item_id = futures[f]
            try:
                raw_url = f.result()
                if raw_url:
                    # Limpiamos la URL
                    final_link = clean_url(raw_url)
                    # Guardamos para el DataFrame
                    results_list.append({"id": item_id, "link": final_link})
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
    df.to_sql('images', con=engine, if_exists='replace', index=False)
    # Terminamos la función
    return


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
    # Obtenemos el link de las imagenes de cada id
    try:
        print("\t • Starting image scraping")
        geckodriver_path = GeckoDriverManager().install()
        df = scraping(unique_ids, geckodriver_path, timeout, headless, user_mail, user_password, max_workers)
    except Exception as e:
        print("\t ❌ Failed to perform scraping for image download")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    try:
        print("\t • Uploading link images to database")
        upload_data(df, db_user, db_user_password, db_host, db_port, db_name)
        print("\t\tData uploaded successfully")
    except Exception as e:
        print("\t ❌ Failed to upload data to database")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función regresando el resultado
    return Result(result=True)
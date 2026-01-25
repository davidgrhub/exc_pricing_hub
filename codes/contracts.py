from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, text, false
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


# Funciones para scraping
def get_driver(geckodriver_path: str, headless: bool, downloads_path: str, delegation: str,
               timeout: int) -> tuple[webdriver, WebDriverWait]:
    # Declaramos si el sistema operativo es windows
    is_windows = platform.system() == "Windows"
    # Declaramos el servicio del driver
    service = Service(geckodriver_path)
    # Configuramos las opciones
    options = webdriver.FirefoxOptions()
    options.set_preference("intl.accept_languages", "en-US,en")
    options.add_argument("-private")
    firefox_exe = r"C:\Program Files\Mozilla Firefox\firefox.exe" if is_windows else "/usr/bin/firefox"
    options.binary_location = firefox_exe
    # Configuramos el headless
    if not (is_windows and headless is False):
        options.add_argument("--headless")
    # Opciones de descarga
    temp_path = os.path.join(downloads_path, delegation)
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", temp_path)
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    # Iniciamos el driver
    driver = webdriver.Firefox(options=options, service=service)
    # Ingresamos el tiempo de espera
    wait = WebDriverWait(driver, timeout)
    # Terminamos la función regresando driver y wait
    return driver, wait


def sing_in(wait: WebDriverWait, user_mail: str, user_password: str) -> None:
    # Ingresamos el correo
    mail = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@id="email"]')))
    mail.click()
    mail.send_keys(user_mail)
    time.sleep(1.5)
    # Seleccionamos submit
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//button[@id="submitBtn"]'))).click()
    # Ingresamos la contraseña
    password = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="password"]')))
    password.click()
    password.send_keys(user_password)
    time.sleep(1.5)
    # Seleccionamos iniciar sesión
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="submit"]'))).click()
    # Mantenemos la sesión iniciada
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@type="submit"]'))).click()
    # Terminamos la función
    return


def filter_delegation(driver: webdriver, timeout: int, delegation: str) -> bool:
    # Cambiamos el tiempo de espera en este bloque
    wait = WebDriverWait(driver, (timeout * 10))
    # Desplegamos la lista de delegaciones
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//i[@class="dropdown-chevron powervisuals-glyph chevron-down"]'))).click()
    # Contador auxiliar
    aux_count = 1
    # Listado auxiliar de delegaciones
    aux_delegation_list = []
    # Variable de control
    flag = False
    # Repasamos las delegaciones del filtro
    while True:
        # Guardamos en una variable temporal
        temp_element = wait.until(ec.visibility_of_element_located(
            (By.XPATH, f'(//span[@class="slicerText"])[{aux_count}]')))
        # Comprobamos que no este la delegación en la lista
        if not temp_element.text in aux_delegation_list:
            # Agregamos a la lista
            aux_delegation_list.append(temp_element.text)
            # Si la delegación es la que buscamos
            if temp_element.text == delegation:
                # Obtenemos el estado de la opción
                val = wait.until(ec.visibility_of_element_located(
                    (By.XPATH, f'(//div[@class="slicerItemContainer"][@title="{temp_element.text}"])')))
                # Comprobamos que no este seleccionada la opción
                if val.get_attribute('aria-selected') == 'false':
                    # Seleccionamos la delegación
                    wait.until(ec.visibility_of_element_located(
                        (By.XPATH, f'(//span[@class="slicerText"])[{aux_count}]'))).click()
                # Salimos del while
                flag = True
                break
            # Si la delegación no es la que buscamos
            else:
                # Pasamos a la siguiente opción
                webdriver.ActionChains(driver).send_keys(Keys.DOWN).perform()
                time.sleep(1.2)
                # Sumamos el contador como máximo 10 y lo mantenemos asi
                if aux_count < 8:
                    aux_count += 1
        # Si la opción ya se encuentra en la lista
        elif temp_element in aux_delegation_list:
            # Cerramos el ciclo while
            break
    # Cerramos los filtros
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//div[@aria-label="Power BI Report"]'))).click()
    # Terminamos la función regresando la bandera
    return flag


def download_data(driver: webdriver, timeout: int) -> None:
    # Declaramos el tiempo máximo de espera del bloque
    wait = WebDriverWait(driver, timeout)
    # Seleccionamos más opciones
    more_options = wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//button[@data-testid="visual-more-options-btn"]')))
    driver.execute_script("arguments[0].click();", more_options)
    # Seleccionamos exportar datos
    export_data = wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//button[@data-testid="pbimenu-item.Export data"]')))
    driver.execute_script("arguments[0].click();", export_data)
    # Seleccionamos exportar
    export = wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//button[@data-testid="export-btn"]')))
    driver.execute_script("arguments[0].click();", export)
    # Cambiamos el tiempo máximo de espera
    wait = WebDriverWait(driver, (timeout * 10))
    # Esperamos a que se descargue el archivo
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//h2[@data-testid="toast-notification-title" and '
                   f'normalize-space(text())="Successful export"]')))
    time.sleep(2)
    # Terminamos la función
    return


def refactor_data(downloads_path: str, delegation: str) -> bool:
    # Declaramos las rutas
    data_path = os.path.join(downloads_path, delegation, 'data.xlsx')
    refactor_path = os.path.join(downloads_path, f'{delegation}.xlsx')
    time.sleep(10)
    # Comprobamos que el archivo exista
    if os.path.exists(data_path):
        # Renombramos y reubicamos el archivo
        os.rename(data_path, refactor_path)
        # Eliminamos la carpeta temporal
        shutil.rmtree(os.path.join(downloads_path, delegation))
        # Terminamos la función regresando el booleano
        return True
    else:
        # Terminamos la función regresando el booleano
        return False


def run_scraping(delegation: str, geckodriver_path: str, headless: bool, downloads_path: str,
                 timeout: int, user_mail: str, user_password: str) -> None:
    # Iniciamos el log
    print_value = f"\t\t{delegation}:"
    # Inicializamos el driver
    driver, wait = get_driver(geckodriver_path, headless, downloads_path, delegation, timeout)
    # Ingresamos a la url del dash
    driver.get('https://app.powerbi.com/groups/3bed2196-69fa-4b00-a42c-3ba9b23d3f69/reports/'
               'a5b968a2-70a0-4cbb-8a16-d8967e1b12dc/30a19dcca000a2c74e73?experience=power-bi')
    # Manejo de error para cerrar el driver
    try:
        # Iniciamos sesión en BI
        sing_in(wait, user_mail, user_password)
        print_value += "\n\t\t\tLogged in successfully"
        # Filtramos la delegacion
        if filter_delegation(driver, timeout, delegation):
            print_value += "\n\t\t\tDelegation was found"
            # Descargamos los contratos de la delegación
            download_data(driver, timeout)
            print_value += "\n\t\t\tReport exported successfully"
            # Movemos el archivo descargado y lo renombramos
            if refactor_data(downloads_path, delegation):
                print_value += "\n\t\t\t✅ Data was refactored successfully"
            # Si no encontramos el archivo descargado
            else:
                print_value += "\n\t\t\t⚠️ No data found"
            # Salimos del scraping
            driver.close()
            driver.quit()
        # Si no encontramos la delegacion
        else:
            print_value += "\n\t\t\t⚠️ Delegation was not found"
    except TimeoutError:
        # Salimos de nuestro scraping cerrando el driver y salimos
        driver.close()
        driver.quit()
        print_value += "\n\t\t\t❌ Scraping failed"
    # Imprimimos le resultado
    print(print_value)
    # Terminamos la función
    return


# Funciones de procesado
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Convertimos los nombres de columnas a nombres válidos
    df.columns = (
        df.columns
        .str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        .str.replace(r'\s+', '_', regex=True).str.replace(r'[^\w]', '', regex=True)
    )
    # Eliminamos las últimas dos filas
    df = df.iloc[:-2]
    # Creamos una columna de ids unicos
    df['unique_id'] = df['product_id'].astype(int).astype(str) + df['option_id'].astype(int).astype(str)
    # Filtramos los servicios
    df = df[df['contract_suplement'] == 'Service']
    # Obtenemos la fecha actual
    current_date = datetime.now()
    # Filtramos los datos con fecha de contrato de servicio válidos
    df = df[((df['fechainisc'] <= current_date) & (df['fechafinsc'] >= current_date))]
    # Ordenamos los datos por "unique_id"
    df = df.sort_values('unique_id', ascending=True)
    # Terminamos la función regresando el dataframe
    return df


def get_final_contracts(df: pd.DataFrame, delegations_dict: dict[int, str]) -> pd.DataFrame:
    # Creamos el dataframe final
    final_data = pd.DataFrame()
    # Procesamos cada 'unique_id' de los contratos
    for unique_id in df['unique_id'].unique():
        # Filtramos la data por cada 'unique_id'
        is_data = df[df['unique_id'] == unique_id]
        # Ordenamos por la columna 'rango_minpax' de menor a mayor
        is_data = is_data.sort_values(by='rango_minpax', ascending=True).reset_index(drop=True)
        # Declaramos la variable de precio
        value = None
        # Comprobamos la variable 'Base' o 'Adult'
        if pd.notna(is_data.iloc[0]['sale_base_usd']) and pd.notna(is_data.iloc[0]['cost_base_usd']):
            value = "base"
        if pd.notna(is_data.iloc[0]['sale_adu_usd']) and pd.notna(is_data.iloc[0]['cost_adu_usd']):
            value = "adu"
        # Sí tenemos variable de costo y venta
        if value is not None:
            new_row = {
                'unique_id': int(unique_id),
                'delegation_id': delegations_dict.get(is_data.iloc[0]['delegation']),
                'delegation_name': is_data.iloc[0]['delegation'],
                'supplier': is_data.iloc[0]['supplier'],
                'product_id': int(is_data.iloc[0]['product_id']),
                'product_name': is_data.iloc[0]['product_name'],
                'option_id': is_data.iloc[0]['option_id'],
                'option_name': is_data.iloc[0]['option_name'],
                'rango_minpax': is_data.iloc[0]['rango_minpax'],
                'rango_maxpax': is_data.iloc[0]['rango_maxpax'],
                'base_or_adult': value.upper(),
                'cost': round(is_data.iloc[0]['cost_' + value + '_usd'], 2),
                'sale': round(is_data.iloc[0]['sale_' + value + '_usd'], 2),
                'margin': round(
                    (is_data.iloc[0]['sale_' + value + '_usd'] - is_data.iloc[0]['cost_' + value + '_usd'])
                    / is_data.iloc[0]['sale_' + value + '_usd'], 2)
            }
            final_data = pd.concat([final_data, pd.DataFrame([new_row])], ignore_index=True)
    # Terminamos la función regresando el dataframe
    return final_data


def process_data(delegation_list: list[str], downloads_paths: str, delegations_dict: dict[int, str]) -> pd.DataFrame:
    # Ignoramos Warnings
    warnings.filterwarnings("ignore")
    # Lista de dataframes
    all_dfs = []
    # Procesamos cada una de las delegaciones
    for delegation in delegation_list:
        # Creamos el path del archivo
        excel_path = os.path.join(downloads_paths, f"{delegation}.xlsx")
        # Comprobamos si existe el archivo
        if os.path.exists(excel_path):
            # Leemos el archivo
            df = pd.read_excel(os.path.join(downloads_path, f'{delegation}.xlsx'))
            # Limpiamos los contratos
            df = clean_data(df)
            # Reestructura de contratos
            df = get_final_contracts(df, delegations_dict)
        else:
            print(f"\t\tFile not found for delegation {delegation}")
    # Terminamos la función regresando el dataframe final
    return


# Función main
def main_contracts(db_user: str, db_user_password: str, db_host: str, db_port: str, db_name: str,
                   headless: bool, timeout: int, user_mail: str, user_password: str, max_workers: int) -> Result:
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
    # Iniciamos el scraping de las delegaciones
    try:
        print("\t • Starting delegations scraping")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(run_scraping, delegation, geckodriver_path, headless, downloads_path, timeout,
                                user_mail, user_password)
                for delegation in delegation_list
            ]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"\t\t❌ Worker failed: {type(e).__name__}: {e}")
    except Exception as e:
        print("\t ❌ Failed to perform scraping for contract download")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Iniciamos el procesado de las delegaciones


    # Terminamos la función regresando el resultado
    return Result(result=True)
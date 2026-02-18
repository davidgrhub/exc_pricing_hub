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
import numpy as np
import platform
import warnings
import shutil
import time
import re
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


def get_activate_delegations(db_user: str, db_user_password: str, db_host: str, db_port: int,
                             db_name: str) -> tuple[dict[int, str], list[str]]:
    # Cadena de conexión
    connection_string = f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}"
    # Creamos el engine
    engine = create_engine(connection_string)
    # Creamos la cadena de petición
    query = text("SELECT delegation_id, delegation_name FROM delegations WHERE is_active = 1")
    # Creamos la lista final
    delegation_list = []
    # Creamos la conexión
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
    # Creamos la lista con el resultado
    for delegation_id, delegation_name in result:
        delegation_list.append(delegation_name)
    # Terminamos la función regresando la lista de nombres
    return delegation_list


def get_required_strategies(db_user: str, db_user_password: str, db_host: str, db_port: int,
                             db_name: str) -> pd.DataFrame:
    # Cadena de conexión
    connection_string = f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}"
    # Creamos el engine
    engine = create_engine(connection_string)
    # Creamos la conexión
    with engine.connect() as conn:
        # Lista de unique_id
        query_ids = text("SELECT unique_id FROM final_discounts_ok")
        result_ids = conn.execute(query_ids).fetchall()
        unique_ids = [row[0] for row in result_ids]
        # DataFrame final_strategies
        final_strategies_df = pd.read_sql("SELECT * FROM final_strategies", conn)
        # DataFrame de sales
        final_sales = pd.read_sql("SELECT * FROM sales_data", conn)
    # Filtrar filas que NO están en final_discounts_ok
    mask_missing = ~final_strategies_df["unique_id"].isin(unique_ids)
    # Aplicar reglas a las filas de la mascara
    final_strategies_df.loc[mask_missing, "final_discount"] = 0.0
    final_strategies_df.loc[mask_missing, "final_sale"] = final_strategies_df.loc[mask_missing, "sale"]
    final_strategies_df.loc[mask_missing, "final_margin"] = final_strategies_df.loc[mask_missing, "margin"]
    # Terminamos la función regresando el dataframe
    return final_strategies_df, final_sales


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
    firefox_exe = "/usr/bin/firefox-esr" if not is_windows else r"C:\Program Files\Mozilla Firefox\firefox.exe"
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
                    (By.XPATH, f'//div[contains(@class, "slicerItemContainer") and @title="{temp_element.text}"]')))
                # Comprobamos que no este seleccionada la opción
                if val.get_attribute('aria-selected') == 'false' or val.get_attribute('aria-checked') == 'false':
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
                if aux_count < 7:
                    aux_count += 1
        # Si la opción ya se encuentra en la lista
        elif temp_element.text in aux_delegation_list:
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
    # Manejo de error para cerrar el driver
    try:
        # Ingresamos a la url del dash
        driver.get('https://app.powerbi.com/links/G-0EQeHMhR?ctid=34b7220f-ddb0-49fb-b389-f4b8d3e1ec9a'
                   '&pbi_source=linkShare')
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
            # Salimos de nuestro scraping cerrando el driver y salimos
            driver.close()
            driver.quit()
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
def clean_column(name):
    # Estandarizamos nombre de columnas
    name = name.lower()
    name = name.replace("%", "percent")
    name = name.replace("&", "")
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name)
    # Regresamos
    return name.strip("_")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Estandarizamos columnas
    df.columns = ["_".join([str(a).strip() for a in col if pd.notna(a) and str(a).strip() != ""])
                  for col in df.columns]
    df.columns = [clean_column(col) for col in df.columns]
    # Renombrar columnas específicas
    df = df.rename(columns={"channel_with_cc_del_nombre": "delegation_name",
                            "channel_with_cc_rsg_sercodigo": "rsg_sercode"})
    # Eliminamos las columnas de margin
    df = df.loc[:, ~df.columns.str.contains("marg")]
    # Eliminar filas completamente vacías
    df = df.dropna(how="all")
    # Eliminar filas que sean "total"
    mask_total = df.apply(lambda r: r.astype(str).str.fullmatch(r'(?i)\s*total\s*').any(), axis=1)
    df = df[~mask_total]
    # Eliminar footer (si existe)
    mask_footer = df.apply(lambda r: r.astype(str).str.contains(r'(?i)applied filters|produced by|report',
                                                                regex=True).any(), axis=1)
    df = df[~mask_footer]
    # Reiniciamos el index
    df = df.reset_index(drop=True)
    # Terminamos la función regresando el dataframe
    return df


def process_data(delegation_list: list[str], downloads_paths: str) -> pd.DataFrame:
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
            df = pd.read_excel(os.path.join(downloads_paths, f'{delegation}.xlsx'), header=[0, 1])
            # Limpiamos los contratos
            df = clean_data(df)
            # Guardamos en la lista de dfs
            all_dfs.append(df)
        else:
            print(f"\t\tFile not found for delegation {delegation}")
    # Unimos todos en un dataframe final
    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    # Terminamos la funcion regresando el dataframe final
    return final_df


# Función para subir la data
def upload_data(df: pd.DataFrame, db_user: str, db_user_password: str, db_host: str, db_port: int,
                db_name: str, table_name: str) -> None:
    # Creamos la conexión
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}")
    # Agregamos el dataframe a la base de datos
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    # Terminamos la función
    return


# Funcion para generar el scorecard
def safe_to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def scaled_points(values: pd.Series, weight: float, ref_u: float) -> pd.Series:
    # Comprobamos que sea numérico
    v = safe_to_numeric(values).fillna(0.0)
    # Referencia por percentil u (valor real del set)
    ref = v.quantile(ref_u, interpolation="higher") if len(v) else 0.0
    # si ref <= 0, no tiene sentido escalar (evita divisiones raras)
    if not np.isfinite(ref) or ref <= 0:
        return pd.Series(np.zeros(len(v)), index=v.index, dtype=float)
    # Ratio
    ratio = (v / ref).clip(lower=0, upper=1)
    # Regresamos la ponderacion
    return weight * ratio


def get_scorecard(strategies_data: pd.DataFrame, sales_data: pd.DataFrame, u: float, w_nm: float,
                  w_m: float, priority_product: list[int], w_p: float, priority_suppliers: list[str],
                  w_s: float, w_in: float, w_bk: float) -> pd.DataFrame:
    # Hacemos una copia de los dataframes
    strat = strategies_data.copy()
    sales = sales_data.copy()
    # Creamos el margen nominal
    strat["nominal_margin"] = (safe_to_numeric(strat["final_sale"]) - safe_to_numeric(strat["cost"])).round(4)
    # Mascara costo mayor a 1
    mask_strat = safe_to_numeric(strat["cost"]).fillna(0.0) > 1
    # Ponderación de margen
    strat["pts_margin"] = 0.0
    strat.loc[mask_strat, "pts_margin"] = (
        scaled_points(strat.loc[mask_strat, "final_margin"], w_m, u).round(4))
    # Ponderación de margen nomial
    strat["pts_nominal_margin"] = 0.0
    strat.loc[mask_strat, "pts_nominal_margin"] = (
        scaled_points(strat.loc[mask_strat, "nominal_margin"], w_nm, u).round(4))
    # Ponderación producto
    strat["pts_product"] = np.where(strat["product_id"].astype(int).isin(priority_product), w_p, 0.0)
    # Ponderaciones de proveedor
    strat["pts_supplier"] = np.where(strat["supplier"].astype(str).isin([str(x) for x in priority_suppliers]),
                                     w_s, 0.0)
    # Columnas de sales necesarias
    sales_cols = ["b2b_off_income_usd", "b2b_off_booking_qty", "callcenter_income_usd", "callcenter_booking_qty",
                  "nexus_web_whitelabel_income_usd", "nexus_web_whitelabel_booking_qty", "nexusgo_income_usd",
                  "nexusgo_booking_qty", "total_income_usd", "total_booking_qty", "nexusapp_income_usd",
                  "nexusapp_booking_qty"]
    # Normalizamos las llaves de union
    strat["product_key"] = safe_to_numeric(strat["product_id"]).fillna(0).astype("Int64").astype(str)
    sales["product_key"] = safe_to_numeric(sales["rsg_sercode"]).fillna(0).astype("Int64").astype(str)
    # Si tenemos ids duplicados sumamos
    sales_agg = ( sales.groupby("product_key", as_index=False)[sales_cols] .sum(numeric_only=True))
    # Unimos las ventas por canal MERGE
    strat = strat.merge(sales_agg, on="product_key", how="left")
    # Limpiamos filas y redondeo
    strat[sales_cols] = strat[sales_cols].apply(safe_to_numeric).fillna(0.0).round(2)
    # Eliminamos la fila de la llave
    strat.drop(columns=["product_key"], inplace=True)
    # Listado de los canales de venta
    chanel_list = ["b2b_off", "callcenter", "nexus_web_whitelabel", "nexusgo", "total", "nexusapp"]
    # Pasamos por cada canal
    for chanel in chanel_list:
        # Ponderación del income
        strat[f"pts_{chanel}_income"] = scaled_points(strat[f"{chanel}_income_usd"], w_in, u).round(4)
        # Ponderación de los bookings
        strat[f"pts_{chanel}_bookings"] = scaled_points(strat[f"{chanel}_booking_qty"], w_bk, u).round(4)
        # Ponderación final
        strat[f"{chanel}_priority"] = (strat["pts_margin"] + strat["pts_nominal_margin"] + strat["pts_product"] +
                                       strat["pts_supplier"] + strat[f"pts_{chanel}_income"] +
                                       strat[f"pts_{chanel}_bookings"]).round(4)
    # Terminamos la función
    return strat


# Función main
def main_scorecard(db_user: str, db_user_password : str, db_host: str, db_port: str, db_name: str,
                   headless: bool, timeout: str, user_mail: str, user_password: str,
                   max_workers: int, u: float, w_nm: float, w_m: float, priority_product: list[int],
                   w_p: float, priority_suppliers: list[str], w_s: float, w_in: float, w_bk: float,
                   scraping: bool) -> Result:
    print("\t[Scorecard Block] Priority Ranking 📊")
    if scraping:
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
            delegation_list = get_activate_delegations(db_user, db_user_password, db_host, db_port, db_name)
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
        try:
            print("\t • Starting delegations processing")
            sales_data_df = process_data(delegation_list, downloads_path)
            print("\t\tFinal data generated successfully")
        except Exception as e:
            print("\t ❌ Failed to generate final data")
            return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
        # Iniciamos el proceso para subir la data en la base de datos
        try:
            print("\t • Uploading final data to database")
            upload_data(sales_data_df, db_user, db_user_password, db_host, db_port, db_name, 'sales_data')
            print("\t\tData uploaded successfully")
        except Exception as e:
            print("\t ❌ Failed to upload data to database")
            return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Obtenemos la data que usaremos para el scorecard
    try:
        strategies_data, sales_data_df = get_required_strategies(db_user, db_user_password, db_host, db_port, db_name)
        print(f"\t • Strategies loaded successfully ({len(strategies_data)})")
    except Exception as e:
        print(f"\t ❌ Failed to get strategies data")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Creamos el scorecard
    try:
        print("\t • Generating weighted scorecard")
        df_scorecard = get_scorecard(strategies_data, sales_data_df, u, w_nm, w_m, priority_product, w_p,
                                     priority_suppliers, w_s, w_in, w_bk)
        print("\t\tWeighted scorecard generated successfully")
    except Exception as e:
        print(f"\t ❌ Failed to generate weighted scorecard")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Iniciamos el proceso para subir la data en la base de datos
    try:
        print("\t • Uploading final scorecard")
        upload_data(df_scorecard, db_user, db_user_password, db_host, db_port, db_name, 'scorecard')
        print("\t\tScorecard uploaded successfully")
    except Exception as e:
        print("\t ❌ Failed to upload scorecard to database")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función regresando el resultado
    return Result(result=True)
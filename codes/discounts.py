from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.keys import Keys
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


# Clase para resultado
@dataclass
class Result:
    result: bool
    error: str | None = None


# Funciones auxiliares
def get_strategies(db_user: str, db_user_password: str, db_host: str, db_port: int, db_name: str) -> pd.DataFrame:
    # Creamos la conexión
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}")
    # Leemos la tabla y la convertimos en DataFrame
    df = pd.read_sql(f"SELECT * FROM final_strategies", con=engine)
    # Terminamos la función regresando el DataFrame
    return df


def cleaning_data(df: pd.DataFrame, ids_off: list[int]) -> pd.DataFrame:
    # Filtramos donde los descuentos y costos sean mayores a 0
    df = df[(df['final_discount'] > 0) & (df['cost'] > 1)]
    # Configuramos las columnas y sus formatos
    df['unique_id'] = df['unique_id'].astype(int)
    df['product_id'] = df['product_id'].astype(int)
    df['option_id'] = df['option_id'].astype(int)
    # Eliminamos los ids de la lista de productos no aplicables
    df = df[~df['product_id'].isin(ids_off)].reset_index(drop=True)
    # Terminamos la función regresando el dataframe filtrado
    return df


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


def sing_in(driver: webdriver, wait: WebDriverWait, user: str, use_password: str) -> None:
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
    password.send_keys(use_password)
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
    # Redirigimos a los descuentos
    driver.get('https://www.nexustours.com/Intranet/descuentos/default.aspx')
    # Terminamos la función
    return


def search_box(wait: WebDriverWait, box: str, active_value: str) -> None:
    time.sleep(2)
    # Buscamos el nombre de la estrategia
    discount_name = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@id="FNombreDescuento-inputEl"]')))
    discount_name.click()
    discount_name.send_keys(box)
    time.sleep(1.2)
    # Modificamos el filtro Activo/Inactivo
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//div[@id="ext-gen1220"]'))).click()
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//div[@data-qtip="{active_value}"]'))).click()
    # Buscamos
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, f'//button[@id="button-1023-btnEl"]'))).click()
    # Esperamos a que cargue la búsqueda
    wait.until(ec.invisibility_of_element_located(
        (By.XPATH, '//div[id="loadmask-1068-msgEl"]')))
    time.sleep(5)
    # Terminamos la función
    return


def deactivate_box(wait: WebDriverWait, strategy: str, count: int) -> int:
    # Abrimos la edición
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//img[@data-qtip="Edit"]'))).click()
    # Comprobamos el nombre de la caja
    block_name = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//div[@id="nombreGrid-body"]')))
    name_box = block_name.find_element(By.XPATH, './div/table/tbody/tr[2]/td[2]/div').text
    # Comprobamos que el nombre coincida con la estrategia
    if name_box == strategy:
        # Desactivamos la caja
        wait.until(ec.visibility_of_element_located(
            (By.XPATH, '//input[@id="DescuentoActivo-inputEl"]'))).click()
        # Guardamos
        wait.until(ec.visibility_of_element_located(
            (By.XPATH, '//button[@id="guardarCerrar-btnEl"]'))).click()
        # Aceptamos
        wait.until(ec.visibility_of_element_located(
            (By.XPATH, '//button[@id="button-1005-btnEl"]'))).click()
        # Agregamos uno al contador
        count += 1
        time.sleep(5)
    # Terminamos la función
    return count


def close_driver(driver: webdriver) -> None:
    # Cerramos el driver
    driver.close()
    # Quitamos la sesión del driver
    driver.quit()
    # Terminamos la función
    return


def run_deactivate(geckodriver_path: str, timeout: int, headless: bool, strategy: str, user: str,
                   use_password: str) -> None:
    # Obtenemos el driver
    driver, wait = get_driver(geckodriver_path, headless, timeout)
    # Iniciamos sesión
    sing_in(driver, wait, user, use_password)
    # Buscamos las cajas activas de la estrategia
    search_box(wait, strategy, "Active")
    # Contador de cajas
    box_count = 0
    # Comprobamos que tengamos cajas activas
    result_search = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//div[@id="tbtext-1065"]'))).text
    # Si no tenemos cajas activas
    if result_search == "No data to display":
        print("\t\t\t\tNo active boxes found for this strategy")
    # Si tenemos cajas activas
    else:
        # Buscamos el total de cajas a desactivar
        box_value = int(result_search.split()[-1])
        # Desactivamos caja por caja
        for _ in range(box_value):
            box_count = deactivate_box(wait, strategy, box_count)
        if box_count == box_value:
            print(f"\t\t\t\tSuccessfully deactivated: {box_count} discount boxes")
        else:
            print(f"\t\t\t\tDeactivated: {box_count} of {box_value} boxes")
    # Cerramos el driver
    close_driver(driver)
    # Terminamos la función
    return


def get_id_box(discount: float, strategy: str) -> str:
    # Menu de las cajas de descuento
    menu_box = {
        # Estrategia EXCFDIS
        'EXCFDIS 0.01': '9189',
        'EXCFDIS 0.02': '9190',
        'EXCFDIS 0.03': '9191',
        'EXCFDIS 0.04': '9192',
        'EXCFDIS 0.05': '9193',
        'EXCFDIS 0.06': '9194',
        'EXCFDIS 0.07': '9195',
        'EXCFDIS 0.08': '9196',
        'EXCFDIS 0.09': '9197',
        'EXCFDIS 0.1': '9198',
        'EXCFDIS 0.11': '9199',
        'EXCFDIS 0.12': '9200',
        'EXCFDIS 0.13': '9201',
        'EXCFDIS 0.14': '9202',
        'EXCFDIS 0.15': '9203',
        'EXCFDIS 0.16': '9204',
        'EXCFDIS 0.17': '9205',
        'EXCFDIS 0.18': '9206',
        'EXCFDIS 0.19': '9208',
        'EXCFDIS 0.2': '9209',
        'EXCFDIS 0.21': '9433',
        'EXCFDIS 0.22': '9434',
        'EXCFDIS 0.23': '9435',
        'EXCFDIS 0.24': '9436',
        'EXCFDIS 0.25': '9437',
        'EXCFDIS 0.26': '9438',
        'EXCFDIS 0.27': '9439',
        'EXCFDIS 0.28': '9440',
        'EXCFDIS 0.29': '9441',
        'EXCFDIS 0.3': '9442',
        'EXCFDIS 0.31': '9443',
        'EXCFDIS 0.32': '9444',
        'EXCFDIS 0.33': '9445',
        'EXCFDIS 0.34': '9446',
        'EXCFDIS 0.35': '9447',

        # Estrategia EXCFCCDIS
        'EXCFCCDIS 0.01': '9281',
        'EXCFCCDIS 0.02': '9282',
        'EXCFCCDIS 0.03': '9283',
        'EXCFCCDIS 0.04': '9284',
        'EXCFCCDIS 0.05': '9285',
        'EXCFCCDIS 0.06': '9286',
        'EXCFCCDIS 0.07': '9287',
        'EXCFCCDIS 0.08': '9288',
        'EXCFCCDIS 0.09': '9289',
        'EXCFCCDIS 0.1': '9290',
        'EXCFCCDIS 0.11': '9291',
        'EXCFCCDIS 0.12': '9292',
        'EXCFCCDIS 0.13': '9293',
        'EXCFCCDIS 0.14': '9294',
        'EXCFCCDIS 0.15': '9295',
        'EXCFCCDIS 0.16': '9296',
        'EXCFCCDIS 0.17': '9297',
        'EXCFCCDIS 0.18': '9298',
        'EXCFCCDIS 0.19': '9299',
        'EXCFCCDIS 0.2': '9300',
        'EXCFCCDIS 0.21': '9418',
        'EXCFCCDIS 0.22': '9419',
        'EXCFCCDIS 0.23': '9420',
        'EXCFCCDIS 0.24': '9421',
        'EXCFCCDIS 0.25': '9422',
        'EXCFCCDIS 0.26': '9423',
        'EXCFCCDIS 0.27': '9424',
        'EXCFCCDIS 0.28': '9425',
        'EXCFCCDIS 0.29': '9426',
        'EXCFCCDIS 0.3': '9427',
        'EXCFCCDIS 0.31': '9428',
        'EXCFCCDIS 0.32': '9429',
        'EXCFCCDIS 0.33': '9430',
        'EXCFCCDIS 0.34': '9431',
        'EXCFCCDIS 0.35': '9432',

        # Estrategia EXCF&FDIS
        'EXCF&FDIS 0.01': '9608',
        'EXCF&FDIS 0.02': '9609',
        'EXCF&FDIS 0.03': '9610',
        'EXCF&FDIS 0.04': '9611',
        'EXCF&FDIS 0.05': '9612',
        'EXCF&FDIS 0.06': '9613',
        'EXCF&FDIS 0.07': '9614',
        'EXCF&FDIS 0.08': '9615',
        'EXCF&FDIS 0.09': '9616',
        'EXCF&FDIS 0.1': '9617',
        'EXCF&FDIS 0.11': '9618',
        'EXCF&FDIS 0.12': '9619',
        'EXCF&FDIS 0.13': '9620',
        'EXCF&FDIS 0.14': '9621',
        'EXCF&FDIS 0.15': '9622',
        'EXCF&FDIS 0.16': '9623',
        'EXCF&FDIS 0.17': '9624',
        'EXCF&FDIS 0.18': '9625',
        'EXCF&FDIS 0.19': '9626',
        'EXCF&FDIS 0.2': '9627',
        'EXCF&FDIS 0.21': '9628',
        'EXCF&FDIS 0.22': '9629',
        'EXCF&FDIS 0.23': '9630',
        'EXCF&FDIS 0.24': '9631',
        'EXCF&FDIS 0.25': '9632',
        'EXCF&FDIS 0.26': '9633',
        'EXCF&FDIS 0.27': '9634',
        'EXCF&FDIS 0.28': '9635',
        'EXCF&FDIS 0.29': '9636',
        'EXCF&FDIS 0.3': '9637',
        'EXCF&FDIS 0.31': '9638',
        'EXCF&FDIS 0.32': '9639',
        'EXCF&FDIS 0.33': '9640',
        'EXCF&FDIS 0.34': '9641',
        'EXCF&FDIS 0.35': '9642',
        'EXCF&FDIS 0.36': '9643',
        'EXCF&FDIS 0.37': '9644',
        'EXCF&FDIS 0.38': '9645',
        'EXCF&FDIS 0.39': '9646',
        'EXCF&FDIS 0.4': '9647',
        'EXCF&FDIS 0.41': '9648',
        'EXCF&FDIS 0.42': '9649',
        'EXCF&FDIS 0.43': '9650',
        'EXCF&FDIS 0.44': '9651',
        'EXCF&FDIS 0.45': '9652',
        'EXCF&FDIS 0.46': '9653',
        'EXCF&FDIS 0.47': '9654',
        'EXCF&FDIS 0.48': '9655',
        'EXCF&FDIS 0.49': '9656',
        'EXCF&FDIS 0.5': '9657',
        'EXCF&FDIS 0.51': '9658',
        'EXCF&FDIS 0.52': '9659',
        'EXCF&FDIS 0.53': '9660',
        'EXCF&FDIS 0.54': '9661',
        'EXCF&FDIS 0.55': '9662',
        'EXCF&FDIS 0.56': '9663',
        'EXCF&FDIS 0.57': '9664',
        'EXCF&FDIS 0.58': '9665',
        'EXCF&FDIS 0.59': '9666',
        'EXCF&FDIS 0.6': '9667',
        'EXCF&FDIS 0.61': '9668',
        'EXCF&FDIS 0.62': '9669',
        'EXCF&FDIS 0.63': '9670',
        'EXCF&FDIS 0.64': '9671',
        'EXCF&FDIS 0.65': '9672',
        'EXCF&FDIS 0.66': '9673',
        'EXCF&FDIS 0.67': '9674',
        'EXCF&FDIS 0.68': '9675',
        'EXCF&FDIS 0.69': '9676',
        'EXCF&FDIS 0.7': '9677',
        'EXCF&FDIS 0.71': '9678',
        'EXCF&FDIS 0.72': '9679',
        'EXCF&FDIS 0.73': '9680',
        'EXCF&FDIS 0.74': '9681',
        'EXCF&FDIS 0.75': '9682',
        'EXCF&FDIS 0.76': '9683',
        'EXCF&FDIS 0.77': '9684',
        'EXCF&FDIS 0.78': '9685',
        'EXCF&FDIS 0.79': '9686',
        'EXCF&FDIS 0.8': '9687',
    }
    # Buscamos el ID de la caja
    id_box = menu_box.get(f'{strategy} {discount}', None)
    # Terminamos la función regresando el ID
    return id_box


def edit_box(wait: WebDriverWait) -> tuple[str, str]:
    # Editamos la caja
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//img[@data-qtip="Edit"]'))).click()
    # Obtenemos el nombre de la caja
    block_name = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//div[@id="nombreGrid-body"]')))
    name_box = block_name.find_element(By.XPATH, './div/table/tbody/tr[2]/td[2]/div').text
    # Obtenemos el valor de descuento
    value_box = wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//input[@id="valor-inputEl"]'))).get_attribute("value")
    # Terminamos la función regresando los valores
    return name_box, value_box


def apply_discounts(wait: WebDriverWait, strategy: str, discount: float, df: pd.DataFrame,
                    message: str, interval: int, id_box: str, driver: webdriver,
                    timeout: int) -> tuple[str, list[int], list[int]]:
    # Lista de productos cargados
    ok_product = []
    # Lista de productos no cargados
    error_product = []
    # Editamos la caja de descuento
    name_box, value_box = edit_box(wait)
    if name_box == strategy and value_box == f"{discount*100:.0f}":
        # Comprobamos si tiene productos cargados previamente
        try:
            product_remove = wait.until(ec.visibility_of_all_elements_located(
                (By.XPATH, '//img[@data-qtip="Remove"]')))
            # Eliminamos los productos
            for remove in product_remove:
                remove.click()
                time.sleep(0.2)
            # Guardamos los cambios
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//button[@id="guardarCerrar-btnEl"]'))).click()
            # Aceptamos los cambios
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//button[@id="button-1005-btnEl"]'))).click()
            time.sleep(2)
            # Editamos la caja
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//img[@data-qtip="Edit"]'))).click()
        except TimeoutException:
            pass
        # Marca de tiempo guardar y recargar
        nex_interval = datetime.now() + timedelta(minutes=interval)
        # Pasamos por cada uno de los productos
        for _, row in df.iterrows():
            # Comprobamos si pasaron más del tiempo máximo
            if datetime.now() >= nex_interval:
                # Guardamos los cambios
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//button[@id="guardarCerrar-btnEl"]'))).click()
                wait = WebDriverWait(driver, (timeout * 3))
                # Aceptamos los cambios
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//button[@id="button-1005-btnEl"]'))).click()
                wait = WebDriverWait(driver, timeout)
                # Cerramos la sesión
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//li[@title="User"]'))).click()
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//li[@title="Exit"]'))).click()
                # Aceptamos la alerta de confirmación
                alert = wait.until(ec.alert_is_present())
                alert.accept()
                time.sleep(5)
                # Iniciamos sesión nuevamente con azure
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//a[@id="_ctl0_data_holder_LoginAzure"]'))).click()
                # Esperamos a que entre a la intranet
                wait.until(ec.visibility_of_element_located(
                    (By.XPATH, '//input[@id="_ctl0_toolbar_holder_localizador_rapido"]')))
                # Redirigimos a los descuentos
                driver.get('https://www.nexustours.com/Intranet/descuentos/default.aspx')
                # Buscamos la caja nuevamente
                time.sleep(2)
                search_box(wait, id_box, "Inactive")
                # Editamos la caja nuevamente
                time.sleep(2)
                edit_box(wait)
                # Guardamos la nueva marca de tiempo guardar y recargar
                nex_interval = datetime.now() + timedelta(minutes=interval)
            # Desplegamos la lista de tipo de producto
            type_product_container = wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//table[@id="TipoProductoPanel-triggerWrap"]')))
            type_product_container.find_element(By.XPATH, './tbody/tr/td[2]').click()
            # Seleccionamos producto excursiones
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//div[@data-qtip="Excursiones"]'))).click()
            # Desplegamos la lista de productos
            product_select_container = wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//table[@id="ProductosSeleccionables-triggerWrap"]')))
            product_select_container.find_element(By.XPATH, './tbody/tr/td[2]').click()
            # Buscamos el producto por su id
            try:
                wait.until(ec.visibility_of_element_located(
                    (By.CSS_SELECTOR, f'div[data-qtip$=" - {str(int(row["product_id"]))}"'))).click()
            except TimeoutException:
                # Si no lo encontramos lo agregamos a la lista de error
                error_product.append(int(row["unique_id"]))
                # Saltamos al siguiente producto
                continue
            # Desplegamos la lista de opciones del producto
            option_select_container = wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//table[@id="ContratosServicio-triggerWrap"]')))
            option_select_container.find_element(By.XPATH, './tbody/tr/td[2]').click()
            # Buscamos la opción del producto
            try:
                wait.until(ec.visibility_of_element_located(
                    (By.CSS_SELECTOR, f'div[data-qtip^="{str(int(row["option_id"]))} -"'))).click()
            except TimeoutException:
                # Si no lo encontramos lo agregamos a la lista de error
                error_product.append(int(row["unique_id"]))
                # Saltamos al siguiente producto
                continue
            # Cargamos el producto en la caja
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//div[@id="btnCargarProductos"]'))).click()
            # Agregamos el tour a la lista de cargados correctamente
            ok_product.append(int(row["unique_id"]))
        # Mostramos el resultado
        if len(ok_product) == len(df):
            message += "\t\t\t\tAll products were successfully loaded\n"
        else:
            message += (f"\t\t\t\tSuccessfully loaded: {len(ok_product)}\n"
                        f"\t\t\t\tFailed to load: {len(error_product)}\n"
                        f"\t\t\t\tFailed IDs: {error_product}\n")
        # Sí tenemos minimo un producto cargado
        if len(ok_product) > 0:
            # Cambiamos la fecha máxima de booking
            booking_date = wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//input[@id="FechaReservaHasta-inputEl"]')))
            booking_date.click()
            booking_date.clear()
            booking_date.send_keys((datetime.today() + timedelta(30)).strftime('%d/%m/%Y'))
            time.sleep(1.5)
            # Activamos la caja de descuentos
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//input[@id="DescuentoActivo-inputEl"]'))).click()
            # Guardamos los cambios
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//button[@id="guardarCerrar-btnEl"]'))).click()
            # Aceptamos los cambios
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//span[@id="button-1005-btnInnerEl"]'))).click()
            message += "\t\t\t\t✅ Box activated successfully"
        # Si no tenemos como minimo un producto
        else:
            # Cerramos la caja sin guardar
            wait.until(ec.visibility_of_element_located(
                (By.XPATH, '//button[@id="button-1263-btnEl"]'))).click()
            message += "\t\t\t\t❌ Box activation failed"
    # Terminamos la función
    return message, ok_product, error_product


def run_discount(geckodriver_path: str, timeout: int, headless: bool, strategy: str, df: pd.DataFrame,
                 discount: float, interval: int, user: str, use_password: str) -> tuple[list[int], list[int]]:
    # Filtramos la base de datos conforme al descuento
    df = df[df['final_discount'] == discount]
    # Declaramos las listas
    ok_product = []
    error_product = []
    # Comprobamos que tengamos descuentos a cargar
    if not df.empty:
        # Obtenemos el ID de la caja de descuento
        id_box = get_id_box(discount, strategy)
        message = f"\t\t\t\tDiscount {discount*100:.0f}% | Box ID {id_box} | Upload: {len(df)} products\n"
        # Obtenemos el driver
        driver, wait = get_driver(geckodriver_path, headless, timeout)
        # Iniciamos sesión
        sing_in(driver, wait, user, use_password)
        # Buscamos la caja de descuento
        search_box(wait, id_box, "Inactive")
        # Aplicamos los descuentos
        message, ok_product, error_product = apply_discounts(wait, strategy, discount, df, message, interval,
                                                             id_box, driver, timeout)
        # Cerramos el driver
        close_driver(driver)
        # Imprimimos el resultado
        print(message)
    # Terminamos la función
    return ok_product, error_product


def save_info(df: pd.DataFrame, ok_: list[int], error_: list[int], db_user: str, db_user_password: str,
              db_host: str, db_port: int, db_name: str) -> None:
    # Verificamos el tipo de dato del dataframe
    df['unique_id'] = df['unique_id'].astype(int)
    # Filtramos el dataframe
    df_ok = df[df['unique_id'].isin(ok_)]
    df_err = df[df['unique_id'].isin(error_)]
    # Creamos la conexión
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_user_password}@{db_host}:{db_port}/{db_name}")
    # Agregamos el dataframe a la base de datos
    df.to_sql('final_discounts', con=engine, if_exists='replace', index=False)
    df_ok.to_sql('final_discounts_ok', con=engine, if_exists='replace', index=False)
    df_err.to_sql('final_discounts_error', con=engine, if_exists='replace', index=False)
    # Terminamos la función
    return


def run_scraping(df: pd.DataFrame, strategy_list: list[str], timeout: int, headless: bool, interval: int,
                 max_workers: int, user: str, use_password: str, db_user: str, db_user_password: str,
              db_host: str, db_port: int, db_name: str) -> None:
    # Comprobamos que el dataframe no este vacío
    if not df.empty:
        # Definimos la ruta del geckodriver
        geckodriver_path = GeckoDriverManager().install()
        # Obtenemos los valores únicos de descuentos
        print_unique_discounts = sorted(df["final_discount"].unique())
        # Convertimos a string para imprimir
        val_str = ", ".join([f"{v * 100:.0f}%" for v in print_unique_discounts])
        print(f"\t\tDiscounts to upload: {len(df)}\n\t\tUnique discounts: {len(print_unique_discounts)}"
              f"\n\t\tValues: {val_str}")
        # Pasamos por cada estrategia de descuentos
        for strategy in strategy_list:
            # Declaramos las listas para guardar los ids cargados y no cargados
            final_ok_products = []
            final_error_products = []
            print(f"\t\t• Strategy: {strategy}")
            # Desactivamos las cajas de la estrategia
            print(f"\t\t\tScraping to deactivate strategy discounts...")
            run_deactivate(geckodriver_path, timeout, headless, strategy, user, use_password)
            # Obtenemos los descuentos unicos ordenados por su frecuencia
            unique_discounts = list(df["final_discount"].value_counts().index)
            # Cargamos descuento por descuento
            print(f"\t\t\tScraping to apply discounts...")
            with ProcessPoolExecutor(max_workers) as executor:
                futures = []
                for discount in unique_discounts:
                    futures.append(
                        executor.submit(run_discount,geckodriver_path, timeout, headless, strategy, df,
                                        discount, interval, user, use_password)
                    )
                # Procesamos los resultados
                for future in as_completed(futures):
                    ok_product, error_product = future.result()
                    if ok_product: final_ok_products.extend(ok_product)
                    if error_product: final_error_products.extend(error_product)
            # Filtramos y guardamos los nuevos csv
            save_info(df, final_ok_products, final_error_products, db_user, db_user_password, db_host, db_port, db_name)
    # Terminamos la función
    return

# Función main
def main_discounts(db_user: str, db_user_password: str, db_host: str, db_port: int, db_name: str,
                   ids_off: list[int], strategy_list: list[str], timeout: int, headless: bool, interval: int,
                   max_workers: int, user: str, use_password: str) -> Result:
    print("\t[Discounts Block] Discount application 🚀")
    # Obtenemos los descuentos a cargar
    try:
        df = get_strategies(db_user, db_user_password, db_host, db_port, db_name)
        print(f"\t • Strategies successfully retrieved. Rows loaded: {len(df)}")
    except Exception as e:
        print("\t ❌ Failed to retrieve strategies from database")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Limpiamos la data
    try:
        df = cleaning_data(df, ids_off)
        print(f"\t • Dataframe cleaned successfully. Rows remaining: {len(df)}")
    except Exception as e:
        print("\t ❌ Failed to clean dataframe")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Aplicamos los descuentos
    try:
        print("\t • Upload process initiated: preparing discounts for upload...")
        run_scraping(df, strategy_list, timeout, headless, interval, max_workers, user, use_password, db_user,
                     db_user_password, db_host, db_port, db_name)
    except Exception as e:
        print("\t ❌ Failed to apply discounts")
        return Result(result=False, error=f"\t[Error] -> {type(e).__name__}: {e}")
    # Terminamos la función main
    return Result(result=True)
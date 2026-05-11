from dotenv import load_dotenv
import os


# Función auxiliar
def strtobool(value: str) -> bool:
    # Tomamos la variable y la estandarizamos
    v = value.strip().lower()
    # Si es una variable positiva
    if v in ("y", "yes", "t", "true", "on", "1"):
        # Terminamos la función regresando True
        return True
    # Si es una variable negativa
    if v in ("n", "no", "f", "false", "off", "0"):
        # Terminamos la función regresando False
        return False
    # En caso de error
    raise ValueError(f"Invalid truth value: {value}")


# Cargamos el archivo .env
load_dotenv()

# Cargamos las variables para python
contracts = strtobool(os.getenv("CONTRACTS"))
strategies = strtobool(os.getenv("STRATEGIES"))
discounts = strtobool(os.getenv("DISCOUNTS"))
information = strtobool(os.getenv("INFORMATION"))
availability = strtobool(os.getenv("AVAILABILITY"))
scorecard = strtobool(os.getenv("SCORECARD"))

db_user = os.getenv("DB_USER")
db_user_password = os.getenv("DB_USER_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))
db_name = os.getenv("DB_NAME")

user_mail = os.getenv("USER_MAIL")
user_password = os.getenv("USER_PASSWORD")

headless = strtobool(os.getenv("HEADLESS"))
timeout = int(os.getenv("TIMEOUT"))
timeout_discounts = int(os.getenv("TIMEOUT_DISCOUNTS"))
max_workers_contracts = int(os.getenv("MAX_WORKERS_CONTRACTS"))
min_margin = float(os.getenv("MIN_MARGIN"))
max_discount = float(os.getenv("MAX_DISCOUNT"))
raw_ids = os.getenv("IDS_OFF", "")
ids_off = {int(i.strip()) for i in raw_ids.replace("\n", "").split(",") if i.strip()}
strategy_list = list(map(str, os.getenv("STRATEGY_LIST").split(",")))
interval = int(os.getenv("INTERVAL"))
max_workers_discounts = int(os.getenv("MAX_WORKERS_DISCOUNTS"))
max_workers_scorecard = int(os.getenv("MAX_WORKERS_SCORECARD"))
max_workers_information = int(os.getenv("MAX_WORKERS_INFORMATION"))
max_workers_availability = int(os.getenv("MAX_WORKERS_AVAILABILITY"))
u_nm = float(os.getenv("U_NM"))
w_nm = float(os.getenv("W_NM"))
method_nm = os.getenv("METHOD_NM")
u_m = float(os.getenv("U_M"))
w_m = float(os.getenv("W_M"))
method_m = os.getenv("METHOD_M")
priority_product = list(map(int, os.getenv("PRIORITY_PRODUCT").split(",")))
w_p = float(os.getenv("W_P"))
priority_suppliers = list(map(str, os.getenv("PRIORITY_SUPPLIERS").split(",")))
w_s = float(os.getenv("W_S"))
u_in = float(os.getenv("U_IN"))
w_in = float(os.getenv("W_IN"))
method_in = os.getenv("METHOD_IN")
u_bk = float(os.getenv("U_BK"))
w_bk = float(os.getenv("W_BK"))
method_bk = os.getenv("METHOD_BK")
scraping_scorecard = strtobool(os.getenv("SCRAPING_SCORECARD"))
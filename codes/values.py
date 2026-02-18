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
ids_off = list(map(int, os.getenv("IDS_OFF").split(",")))
strategy_list = list(map(str, os.getenv("STRATEGY_LIST").split(",")))
interval = int(os.getenv("INTERVAL"))
max_workers_discounts = int(os.getenv("MAX_WORKERS_DISCOUNTS"))
max_workers_scorecard = int(os.getenv("MAX_WORKERS_SCORECARD"))
u = float(os.getenv("U"))
w_nm = float(os.getenv("W_NM"))
w_m = float(os.getenv("W_M"))
w_p = float(os.getenv("W_P"))
w_s = float(os.getenv("W_S"))
w_in = float(os.getenv("W_IN"))
w_bk = float(os.getenv("W_BK"))
priority_product = list(map(int, os.getenv("PRIORITY_PRODUCT").split(",")))
priority_suppliers = list(map(str, os.getenv("PRIORITY_SUPPLIERS").split(",")))
scraping_scorecard = strtobool(os.getenv("SCRAPING_SCORECARD"))
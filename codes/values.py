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
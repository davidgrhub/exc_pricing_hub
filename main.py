from codes.contracts import Result as ContractsResult
from codes.contracts import main_contracts
import codes.values as values
import time

#
# Función auxiliar
def format_time(start_time: float) -> str:
    # Calcula el tiempo transcurrido desde nuestro tiempo de inicio
    result = time.perf_counter() - start_time
    # Ajustamos el formato de salida
    h, rem = divmod(result, 3600)
    m = rem // 60
    # Terminamos la función regresando el tiempo final
    return f"{int(h)}hours {int(m)}min"


# Función main
def main() -> None:
    print("[MAIN] EXC Pricing Hub 🤖")
    # Iniciamos el temporizador
    start_time = time.perf_counter()
    # Bloque contratos
    if values.contracts:
        # Ejecutamos el bloque de contratos
        result: ContractsResult = main_contracts(values.db_user, values.db_user_password, values.db_host,
                                                 values.db_port, values.db_name, values.headless, values.timeout,
                                                 values.user_mail, values.user_password, values.max_workers)
        # Imprimimos si existe el error
        if not result.result: print(result.error)
    # Imprimimos el tiempo de ejecución total
    print(f"[MAIN] Execution time: {format_time(start_time)}")
    # Terminamos la función
    return


# Main
if __name__ == "__main__":
    main()
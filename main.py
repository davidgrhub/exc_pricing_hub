from codes.contracts import Result as ContractsResult
from codes.contracts import main_contracts
from codes.strategies import Result as StrategiesResult
from codes.strategies import main_strategies
from codes.discounts import Result as DiscountsResult
from codes.discounts import main_discounts
from codes.scorecard import Result as ScorecardResult
from codes.scorecard import main_scorecard
import codes.values as values
import time


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
                                                 values.user_mail, values.user_password, values.max_workers_contracts)
        # Imprimimos si existe el error
        if not result.result: print(result.error)
    # Bloque estrategias
    if values.strategies:
        # Ejecutamos el bloque de contratos
        result: StrategiesResult = main_strategies(values.db_user, values.db_user_password, values.db_host,
                                                   values.db_port, values.db_name, values.min_margin,
                                                   values.max_discount)
        # Imprimimos si existe el error
        if not result.result: print(result.error)
    # Bloque de descuentos
    if values.discounts:
        # Ejecutamos el bloque de descuentos
        result: DiscountsResult = main_discounts(values.db_user, values.db_user_password, values.db_host,
                                                 values.db_port, values.db_name, values.ids_off, values.strategy_list,
                                                 values.timeout_discounts, values.headless, values.interval,
                                                 values.max_workers_discounts, values.user_mail, values.user_password)
        # Imprimimos si existe el error
        if not result.result: print(result.error)
    # Bloque de scorecard
    if values.scorecard:
        # Ejecutamos el bloque de scorecard
        result: ScorecardResult = main_scorecard(values.db_user, values.db_user_password, values.db_host,
                                                 values.db_port, values.db_name, values.headless, values.timeout,
                                                 values.user_mail, values.user_password, values.max_workers_scorecard,
                                                 values.u, values.w_nm, values.w_m, values.priority_product,
                                                 values.w_p, values.priority_suppliers, values.w_s, values.w_in,
                                                 values.w_bk, values.scraping_scorecard)
        # Imprimimos si existe el error
        if not result.result: print(result.error)
    # Imprimimos el tiempo de ejecución total
    print(f"[MAIN] Execution time: {format_time(start_time)}")
    # Terminamos la función


# Main
if __name__ == "__main__":
    main()
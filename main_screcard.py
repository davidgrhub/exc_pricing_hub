from codes.scorecard import Result as ScorecardResult
from codes.scorecard import main_scorecard
import codes.values as values
import os


# Funcion auxiliar
def update_env_file(file_path: str, updates: dict) -> None:
    # Editamos el archivo
    with open(file_path, 'r') as f:
        lines = f.readlines()
    # Diccionario para rastrear qué se actualizó
    updated_content = []
    keys_found = set()
    # Buscamos las líneas
    for line in lines:
        stripped = line.strip()
        # Ignorar comentarios o líneas vacías para el proceso de reemplazo
        if not stripped or stripped.startswith('#'):
            updated_content.append(line)
            continue

        key = stripped.split('=')[0]
        if key in updates:
            updated_content.append(f"{key}={updates[key]}\n")
            keys_found.add(key)
        else:
            updated_content.append(line)
    # Añadir las llaves que no existían en el archivo original
    for key, value in updates.items():
        if key not in keys_found:
            updated_content.append(f"{key}={value}\n")
    # Editamos el archivo
    with open(file_path, 'w') as f:
        f.writelines(updated_content)
    print(f"✅ Archivo {file_path} ile successfully updated")
    # Terminamos la función
    return


# Función main
def main(u: float, w_nm: float, w_m: float, priority_product: list[int], w_p: float, priority_suppliers: list[str],
         w_s: float, w_in: float, w_bk: float) -> None:
    print("[MAIN SCORECARD] EXC Pricing Hub 🤖")
    # Editamos el .env
    env_updates = {
        "U": str(u),
        "W_NM": str(w_nm),
        "W_M": str(w_m),
        "W_P": str(w_p),
        "W_S": str(w_s),
        "W_IN": str(w_in),
        "W_BK": str(w_bk),
        "PRIORITY_PRODUCT": ",".join(map(str, priority_product)),
        "PRIORITY_SUPPLIERS": ",".join(priority_suppliers)
    }
    update_env_file(".env", env_updates)
    # Ejecutamos el bloque de scorecard
    result: ScorecardResult = main_scorecard(values.db_user, values.db_user_password, values.db_host,
                                             values.db_port, values.db_name, values.headless, values.timeout,
                                             values.user_mail, values.user_password, values.max_workers_scorecard,
                                             values.u, values.w_nm, values.w_m, values.priority_product,
                                             values.w_p, values.priority_suppliers, values.w_s, values.w_in,
                                             values.w_bk, False)
    # Imprimimos si existe el error
    if not result.result: print(result.error)
    # Terminamos la función
    return


# Main
if __name__ == "__main__":
    U = 1.0  # Valores del 0 al 1
    W_NM = 0.25  # Ponderación del margen nominal
    W_M = 0.20  # Ponderación del margen
    W_P = 0.05  # Ponderación del producto
    W_S = 0.05  # Ponderación del proveedor
    W_IN = 0.30  # Ponderación del income
    W_BK = 0.15  # Ponderación de los bookings
    PRIORITY_PRODUCT = [10095, 3440]
    PRIORITY_SUPPLIERS = ["Rio Secreto", "4Wheels4Fun"]
    main(U, W_NM, W_M, PRIORITY_PRODUCT, W_P, PRIORITY_SUPPLIERS, W_S, W_IN, W_BK)
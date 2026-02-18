from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import main_scorecard

app = FastAPI()

# Definimos qué datos esperamos recibir
class ScorecardConfig(BaseModel):
    u: float
    w_nm: float
    w_m: float
    w_p: float
    w_s: float
    w_in: float
    w_bk: float
    priority_product: list[int]
    priority_suppliers: list[str]

@app.post("/run-scorecard")
def run_task(config: ScorecardConfig, background_tasks: BackgroundTasks):
    # Usamos BackgroundTasks para que la web no se quede "colgada"
    # esperando a que el proceso de scorecard (que es lento) termine.
    background_tasks.add_task(
        main_scorecard.main,
        config.u, config.w_nm, config.w_m, config.priority_product,
        config.w_p, config.priority_suppliers, config.w_s, config.w_in, config.w_bk
    )
    return {"status": "Proceso iniciado en segundo plano"}
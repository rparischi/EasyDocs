from app.application.usecases.consulta_canhoto import ConsultaCanhotosUseCase
from datetime import datetime
import time
import logging
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/consulta_canhotos.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == "__main__":
    while True:
        try:
            data = datetime.now().strftime("%d/%m/%Y")
            logging.info(f"Iniciando execução para data {data}")
            usecase = ConsultaCanhotosUseCase()
            usecase.executar(data)
            logging.info("Execução concluída com sucesso.")
        except Exception as e:
            logging.error(f"Erro no loop principal: {e}")
        time.sleep(30)

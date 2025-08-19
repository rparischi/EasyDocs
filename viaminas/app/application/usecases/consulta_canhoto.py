from app.infrastructure.external.easydoc_api import EasyDocAPI
from app.infrastructure.database.repository import CanhotoRepository
import logging

class ConsultaCanhotosUseCase:
    def __init__(self):
        self.api = EasyDocAPI()
        self.repo = CanhotoRepository()

    def executar(self, data_processamento):
        try:
            self.api.autenticar()
            for canhoto in self.api.consultar_canhotos(data_processamento):
                self.repo.salvar(canhoto)
        except Exception as e:
            logging.error(f"Erro na execução do usecase: {e}")

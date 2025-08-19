import requests
from datetime import datetime
from app.domain.entities.canhoto import Canhoto
import logging

class EasyDocAPI:
    def __init__(self):
        self.base_url = "https://wsoli.easydocs.com.br"
        self.email = "sync@easydocs.com.br"
        self.senha = "syeasydoc1234@"
        self.cliente = "viaminas"
        self.token = None

    def autenticar(self):
        try:
            response = requests.post(f"{self.base_url}/login", headers={
                "email": self.email,
                "senha": self.senha,
                "client": self.cliente
            })

            response.raise_for_status()
            self.token = response.json()["accessToken"]
            logging.info("Autenticado com sucesso na EasyDoc.")
        except Exception as e:
            logging.error(f"Erro na autenticação EasyDoc: {e}")
            raise

    def consultar_canhotos(self, data_processamento: str):
        try:
            headers = {"Authorization": f"Bearer {self.token}"}
            payload = {
                "tipodocumento": "canhoto",
                "pesquisa": [{"dataprocessamento": data_processamento}],
                "paginacao": {"pagina": "1", "registrosporpagina": "50"},
                "retorno": {
                    "dado": "numeronf,serienf,cnpjemissor,dataentrega,urlimagem,urlimagemprevia",
                    "nomearquivo": "numeronf,serienf,cnpjemissor,dataentrega,urlimagem,urlimagemprevia"
                }
            }
            response = requests.post(f"{self.base_url}/Operation/pesquisametadadodrive", json=payload, headers=headers)
            response.raise_for_status()
            resposta = response.json()
            if isinstance(resposta, list) and len(resposta) > 0:
                metadados = resposta[0].get("Metadado", [])
                for item in metadados:
                    yield Canhoto(
                        item.get("numeronf"),
                        item.get("serienf"),
                        item.get("cnpjemissor"),
                        datetime.strptime(item.get("dataentrega"), "%d/%m/%Y").date() if item.get("dataentrega") else None,
                        item.get("urlimagem"),
                        item.get("urlimagemprevia")
                    )
            else:
                logging.warning("Resposta inesperada ou vazia ao consultar canhotos.")
        except Exception as e:
            logging.error(f"Erro ao consultar canhotos: {e}")
            raise

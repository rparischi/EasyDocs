from app.domain.entities.canhoto import Canhoto
from app.infrastructure.database.connection import get_connection
import logging

class CanhotoRepository:
    def salvar(self, canhoto: Canhoto):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO edi_ocorrencias_entrega (numero_nfe, serie_nfe, cnpj_emitente, data_emissao, urlimagem, urlimagemprevia)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (canhoto.numeronf, canhoto.serienf, canhoto.cnpjemissor, canhoto.dataentrega, canhoto.urlimagem, canhoto.urlimagemprevia))
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Canhoto salvo: {canhoto.numeronf}")
        except Exception as e:
            logging.error(f"Erro ao salvar canhoto: {e}")

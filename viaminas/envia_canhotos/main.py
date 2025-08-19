import time
import psycopg2
import requests
import json
from datetime import datetime

# Configurações do banco
DB_CONFIG = {
    "host": "pg.softlog.eti.br",
    "user": "softlog_viaminas",
    "password": "softlog#321",
    "database": "softlog_viaminas"
}

# Configurações da API
API_URL = "https://apiviaminas.easydocs.com.br/document/uploadjson"
API_TOKEN = "10.42c09f557438b6cfc7f9e99f7f05f5749e9ca2a83f2d45dcdcf20107df37c5f9"

# Consulta SQL (ajuste os campos e filtros para sua tabela)
SQL_SELECT = """
            with c as (
                select codigo_cliente
                from cliente
                where cnpj_cpf like '14408399%'
                or cnpj_cpf like '16619378%'
                or cnpj_cpf like '28287523%'
                or cnpj_cpf like '05651966%'
            )
            select
                nf.id_nota_fiscal_imp
                ,nf.chave_nfe
                ,nf.numero_nota_fiscal
                ,nf.serie_nota_fiscal
                ,r.cnpj_cpf as cnpj_emissor
                ,r.nome_cliente as nome_emissor
                ,nf.data_emissao
                ,d.cnpj_cpf as cnpj_dest
                ,d.nome_cliente as nome_dest
                ,d.endereco as endereco_dest
                ,c.nome_cidade as cidade_dest
                ,c.uf as uf_dest
                ,'30899999000113'::character(14) as cnpj_transp
                ,'VIA MINAS TRANSPORTES E ENCOMENDAS LTDA'::character(50) as nome_transp
                
            from scr_notas_fiscais_imp nf
            left join cliente r ON r.codigo_cliente = nf.remetente_id
            left join cliente d ON d.codigo_cliente = nf.destinatario_id
            left join cidades c ON c.id_cidade::integer = d.id_cidade::integer
            where nf.data_expedicao >= '2025-02-01'
            and nf.remetente_id IN (select codigo_cliente from c)
            AND NOT EXISTS (SELECT 1
                            FROM fila_easydocs fe
                            WHERE fe.id_nota_fiscal_imp = nf.id_nota_fiscal_imp);
            """

# Função para conectar ao banco
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# Função para enviar para a API
def enviar_canhoto(row):
    payload = [
        {
            "document_type_id": 1,
            "document": [
                {
                    "data": {
                        "CHAVENF": row[1],
                        "NUMERONF": row[2],
                        "SERIENF": row[3],
                        "CNPJEMISSOR": row[4].strip(),
                        "NOMEEMISSOR": row[5],
                        "DATAEMISSAO": datetime.strptime(str(row[6]), "%Y-%m-%d").strftime("%d/%m/%Y"),
                        "CNPJDESTINATARIO": row[7].strip(),
                        "NOMEDESTINATARIO": row[8],
                        "ENDERECODESTINATARIO": row[9].strip(),
                        "CIDADEDESTINATARIO": row[10].strip(),
                        "UFDESTINATARIO": row[11],
                        "CNPJTRANSPORTADOR": row[12],
                        "NOMETRANSPORTADOR": row[13].strip()
                    }
                }
            ]
        }
    ]
    
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

    response = requests.post(API_URL, headers=headers, json=payload)

    if response.status_code == 200: #and '"sucess": true' in response.text:
        print(f"Canhoto {row[1]} enviado com sucesso!")
        return True, response.status_code
    else:
        print(f"Falha ao enviar canhoto {row[1]}: {response.text}")
        return False, response.status_code

# Loop do serviço
def main():
    while True:
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(SQL_SELECT)
            registros = cur.fetchall()

            for reg in registros:
                retorno, status = enviar_canhoto(reg)
                
                if retorno:
                    cur.execute(
                    "INSERT INTO fila_easydocs (id_nota_fiscal_imp, data_envio, status_envio) VALUES (%s, %s, %s)",
                    (reg[0], datetime.now(), status))
                    conn.commit()

            cur.close()
            conn.close()
        except Exception as e:
            print("Erro:", e)

        time.sleep(30)  # Aguarda 30 segundos

if __name__ == "__main__":
    main()

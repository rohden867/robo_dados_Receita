import cx_Oracle
import http.client
import json
import time

# Função para conectar ao banco de dados Oracle e obter os CNPJs
def obter_cnpjs_do_banco():
    # Configuração de conexão ao banco de dados Oracle
    user = "rudieri"
    password = "Rud13r12tR0hd3n135"
    dsn = "192.168.1.253:1521/ROHDEN"

    # Consulta SQL que será executada, agora com DISTINCT para evitar CNPJs duplicados
    sql_query = """
    SELECT DISTINCT CLI.NR_CGC
    FROM HDS.NFPRINCIPAL NFP,
         HDS.CLIFOR CLI
    WHERE NFP.CD_CLIFOR = CLI.CD_CLIFOR
    AND NFP.TP_TIPO_NOTA = 'S'
    AND NFP.FG_CANCELAMENTO = 'N'
    AND NFP.DT_EMISSAO = TO_DATE('16/10/2024', 'DD/MM/YYYY')
    AND CLI.NR_CGC NOT IN ('0','01735959000138','05959604000183','05959604000234','75853168000149','75853168000300',
                           '75853168000734','75853168000491','75853168000572','75853168000653','75853168000815',
                           '75853168000904')
    ORDER BY CLI.NR_CGC
    """

    cnpjs = []

    try:
        # Conectando ao banco de dados Oracle
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        print("Conexão bem-sucedida ao banco de dados Oracle!")

        # Criar um cursor para executar a consulta
        cursor = connection.cursor()

        # Executar a consulta SQL
        cursor.execute(sql_query)

        # Ler os CNPJs retornados pela consulta
        for row in cursor:
            cnpj = row[0]
            cnpjs.append(cnpj)

        print(f"{len(cnpjs)} CNPJs únicos obtidos do banco de dados.")

        # Salvar os CNPJs em um arquivo txt
        with open("cnpjs_retornados.txt", "w") as file:
            for cnpj in cnpjs:
                file.write(f"{cnpj}\n")
        print("CNPJs salvos em 'cnpjs_retornados.txt'.")

    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao conectar ou executar a consulta: {e}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Conexão fechada.")

# Função para consultar o CNPJ na API receitaws
def consultar_cnpj(cnpj):
    # Estabelecendo a conexão com a API Receitaws
    conn = http.client.HTTPSConnection("receitaws.com.br")
    
    # Cabeçalhos para a solicitação da API
    headers = {
        'Accept': "application/json"
    }
    
    try:
        # Enviando a solicitação GET para a API com o CNPJ
        conn.request("GET", f"/v1/cnpj/{cnpj}", headers=headers)
        # Obtendo a resposta da API
        res = conn.getresponse()
        # Lendo os dados da resposta
        data = res.read()
        # Decodificando os dados de bytes para string
        decoded_data = data.decode("utf-8")
        # Convertendo os dados decodificados (string JSON) em um dicionário Python
        cnpj_data = json.loads(decoded_data)
        # Fechando a conexão
        conn.close()
        print(f"Consulta do CNPJ {cnpj} bem-sucedida.")
        return cnpj_data
    
    except Exception as e:
        print(f"Erro ao consultar o CNPJ {cnpj}: {e}")
        return None

# Função para salvar os dados do CNPJ em um arquivo JSON
def salvar_dados_cnpj(cnpj, dados):
    # Nome do arquivo JSON baseado no CNPJ
    nome_arquivo = f"cnpj_{cnpj}.json"
    try:
        # Salvando os dados em um arquivo JSON
        with open(nome_arquivo, "w", encoding='utf-8') as json_file:
            json.dump(dados, json_file, ensure_ascii=False, indent=4)
        print(f"Dados do CNPJ {cnpj} salvos em {nome_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar os dados do CNPJ {cnpj}: {e}")

# Função principal para processar os CNPJs do arquivo
def processar_cnpjs():
    # Lendo os CNPJs do arquivo txt
    with open("cnpjs_retornados.txt", "r") as file:
        cnpjs = [line.strip() for line in file.readlines()]
    
    # Processar 2 CNPJs por minuto
    for i in range(0, len(cnpjs), 2):
        # Pegando o lote de até 2 CNPJs
        lote_cnpjs = cnpjs[i:i+2]
        print(f"Processando lote de CNPJs: {lote_cnpjs}")
        
        for cnpj in lote_cnpjs:
            print(f"Consultando CNPJ: {cnpj}")
            # Consultar o CNPJ na API
            dados_cnpj = consultar_cnpj(cnpj)
            if dados_cnpj:
                # Salvar os dados retornados em um arquivo JSON
                salvar_dados_cnpj(cnpj, dados_cnpj)
            else:
                print(f"Não foi possível obter os dados para o CNPJ {cnpj}")
        
        # Aguarda 60 segundos antes de processar o próximo lote de CNPJs
        print(f"Aguardando 1 minuto antes de processar o próximo lote de CNPJs...\n")
        time.sleep(60)

if __name__ == "__main__":
    # Passo 1: Obter os CNPJs do banco de dados e salvar no arquivo
    obter_cnpjs_do_banco()
    
    # Passo 2: Processar os CNPJs do arquivo e consultar na API
    processar_cnpjs()

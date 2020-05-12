#############################################################################################################################################################
# SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG #
#############################################################################################################################################################

import psycopg2


"""
Cria o schema do sub banco de dados do SINAN_DENG (Agravos dengue e chikungunya) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do sub banco de dados sinan_deng
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela de Notificações dos Agravos Dengue e Chikungunya (tabela principal do sub banco de dados sinan_deng)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.dengbr("NU_NOTIFIC"            VARCHAR(7),
                                                                    "UF_DENG"               VARCHAR(2),
                                                                    "ANO_DENG"              INTEGER,
                                                                    "TP_NOT"                VARCHAR(2),
                                                                    "ID_AGRAVO"             VARCHAR(3),
                                                                    "MUNICIP_ID"            VARCHAR(6),
                                                                    "RES_CHIKS1"            VARCHAR(2),
                                                                    "RES_CHIKS2"            VARCHAR(2),
                                                                    "RESUL_PRNT"            VARCHAR(2),
                                                                    "RESUL_SORO"            VARCHAR(2),
                                                                    "RESUL_NS1"             VARCHAR(2),
                                                                    "SOROTIPO"              VARCHAR(2),
                                                                    "HOSPITALIZ"            VARCHAR(2),
                                                                    "CLASSIFIN_ID"          VARCHAR(2),
                                                                    "EVOLUCAO"              VARCHAR(2),
                                                                    "GRAV_PULSO"            NUMERIC,
                                                                    "GRAV_CONV"             NUMERIC,
                                                                    "GRAV_ENCH"             NUMERIC,
                                                                    "GRAV_INSUF"            NUMERIC,
                                                                    "GRAV_TAQUI"            NUMERIC,
                                                                    "GRAV_EXTRE"            NUMERIC,
                                                                    "GRAV_HIPOT"            NUMERIC,
                                                                    "GRAV_HEMAT"            NUMERIC,
                                                                    "GRAV_MELEN"            NUMERIC,
                                                                    "GRAV_METRO"            NUMERIC,
                                                                    "GRAV_SANG"             NUMERIC,
                                                                    "GRAV_AST"              NUMERIC,
                                                                    "GRAV_MIOC"             NUMERIC,
                                                                    "GRAV_CONSC"            NUMERIC,
                                                                    "GRAV_ORGAO"            NUMERIC);

                       /*Tabela dos municípios de notificação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.municip("ID"                   VARCHAR(6),
                                                                     "MUNNOME"	            VARCHAR(66),
                                                                     "MUNNOMEX"             VARCHAR(66),
                                                                     "MUNCODDV"             VARCHAR(7),
                                                                     "OBSERV"               VARCHAR(66),
                                                                     "SITUACAO"             VARCHAR(66),
                                                                     "MUNSINP"	            VARCHAR(66),
                                                                     "MUNSIAFI"             VARCHAR(66),
                                                                     "UFCOD_ID"             VARCHAR(2),
                                                                     "AMAZONIA"             VARCHAR(66),
                                                                     "FRONTEIRA"            VARCHAR(66),
                                                                     "CAPITAL"              VARCHAR(66),
                                                                     "RSAUDE_ID"            VARCHAR(5),
                                                                     "LATITUDE"             FLOAT,
                                                                     "LONGITUDE"            FLOAT,
                                                                     "ALTITUDE"             FLOAT,
                                                                     "AREA"                 FLOAT,
                                                                     "ANOINST"              VARCHAR(66),
                                                                     "ANOEXT"               VARCHAR(66),
                                                                     "SUCESSOR"             VARCHAR(66));

                       /*Tabela dos tipos de classificação da dengue/chikungunya*/
                       CREATE TABLE IF NOT EXISTS {child_db}.classifin("ID"                 VARCHAR(2),
                                                                       "CLASSIFICACAO"      VARCHAR(66));

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                     VARCHAR(2),
                                                                   "ESTADO"                 VARCHAR(66),
                                                                   "SIGLA_UF"               VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                    VARCHAR(5),
                                                                    "REGIAO"                VARCHAR(66));

                       /*Tabela de Arquivos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.arquivos("NOME"                VARCHAR(15),
                                                                      "DIRETORIO"           VARCHAR(66),
                                                                      "DATA_INSERCAO_FTP"   DATE,
                                                                      "DATA_HORA_CARGA"     TIMESTAMP,
                                                                      "QTD_REGISTROS"       INTEGER);

                ''')
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()

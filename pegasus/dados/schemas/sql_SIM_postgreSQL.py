##########################################################################################################################################################
#  SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM  SIM  #
##########################################################################################################################################################

import psycopg2


"""
Cria o schema do banco de dados do SIM para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server
com no máximo pequenas modificações.

"""

# Função de criação das tabelas do banco de dados sim
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Declarações de Óbito (tabela principal do banco de dados sim)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.dobr("NUMERODO"                 VARCHAR(8),
                                                                  "UF_DO"                    VARCHAR(2),
                                                                  "ANO_DO"                   INTEGER,
                                                                  "CODINST"                  VARCHAR(18),
                                                                  "TIPOBITO_ID"              VARCHAR(2),
                                                                  "DTOBITO"                  DATE,
                                                                  "HORAOBITO"                VARCHAR(4),
                                                                  "NUMSUS"                   VARCHAR(15),
                                                                  "NATURALE_ID"              VARCHAR(3),
                                                                  "CODMUNNATU_ID"            VARCHAR(6),
                                                                  "DTNASC"                   DATE,
                                                                  "IDADE"                    FLOAT,
                                                                  "SEXO"                     VARCHAR(2),
                                                                  "RACACOR_ID"               VARCHAR(2),
                                                                  "ESTCIV_ID"                VARCHAR(2),
                                                                  "ESC_ID"                   VARCHAR(2),
                                                                  "ESC2010_ID"               VARCHAR(2),
                                                                  "OCUP_ID"                  VARCHAR(6),
                                                                  "CODMUNRES_ID"             VARCHAR(6),
                                                                  "LOCOCOR_ID"               VARCHAR(2),
                                                                  "CODESTAB_ID"              VARCHAR(7),
                                                                  "CODMUNOCOR_ID"            VARCHAR(6),
                                                                  "TPMORTEOCO_ID"            VARCHAR(2),
                                                                  "ASSISTMED"                NUMERIC,
                                                                  "EXAME"                    NUMERIC,
                                                                  "CIRURGIA"                 NUMERIC,
                                                                  "NECROPSIA"                NUMERIC,
                                                                  "LINHAA"                   VARCHAR(66),
                                                                  "LINHAB"                   VARCHAR(66),
                                                                  "LINHAC"                   VARCHAR(66),
                                                                  "LINHAD"                   VARCHAR(66),
                                                                  "LINHAII"                  VARCHAR(66),
                                                                  "CAUSABAS_ID"              VARCHAR(4),
                                                                  "CRM"                      VARCHAR(15),
                                                                  "DTATESTADO"               DATE,
                                                                  "CIRCOBITO_ID"             VARCHAR(2),
                                                                  "ACIDTRAB"                 NUMERIC,
                                                                  "FONTE_ID"                 VARCHAR(2),
                                                                  "TPPOS"                    NUMERIC,
                                                                  "DTINVESTIG"               DATE,
                                                                  "CAUSABAS_O_ID"            VARCHAR(4),
                                                                  "DTCADASTRO"               DATE,
                                                                  "ATESTANTE_ID"             VARCHAR(2),
                                                                  "FONTEINV_ID"              VARCHAR(2),
                                                                  "DTRECEBIM"                DATE,
                                                                  "ATESTADO"                 VARCHAR(66),
                                                                  "ESCMAEAGR1_ID"            VARCHAR(2),
                                                                  "ESCFALAGR1_ID"            VARCHAR(2),
                                                                  "STDOEPIDEM"               NUMERIC,
                                                                  "STDONOVA"                 NUMERIC,
                                                                  "DIFDATA"                  FLOAT,
                                                                  "DTCADINV"                 DATE,
                                                                  "TPOBITOCOR_ID"            VARCHAR(2),
                                                                  "DTCONINV"                 DATE,
                                                                  "FONTENTREV"               NUMERIC,
                                                                  "FONTEAMBUL"               NUMERIC,
                                                                  "FONTEPRONT"               NUMERIC,
                                                                  "FONTESVO"                 NUMERIC,
                                                                  "FONTEIML"                 NUMERIC,
                                                                  "FONTEPROF"                NUMERIC);

                       /*Tabela do tipo de óbito*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tipobito("ID"                   VARCHAR(2),
                                                                      "TIPO"                 VARCHAR(66));

                       /*Tabela do local de nascimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.naturale("ID"                   VARCHAR(3),
                                                                      "LOCAL"                VARCHAR(66));

                       /*Tabela dos municípios de naturalidade*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunnatu("ID"                 VARCHAR(6),
                                                                        "MUNNOME"	         VARCHAR(66),
                                                                        "MUNNOMEX"           VARCHAR(66),
                                                                        "MUNCODDV"           VARCHAR(7),
                                                                        "OBSERV"             VARCHAR(66),
                                                                        "SITUACAO"           VARCHAR(66),
                                                                        "MUNSINP"	         VARCHAR(66),
                                                                        "MUNSIAFI"           VARCHAR(66),
                                                                        "UFCOD_ID"           VARCHAR(2),
                                                                        "AMAZONIA"           VARCHAR(66),
                                                                        "FRONTEIRA"          VARCHAR(66),
                                                                        "CAPITAL"            VARCHAR(66),
                                                                        "RSAUDE_ID"          VARCHAR(5),
                                                                        "LATITUDE"           FLOAT,
                                                                        "LONGITUDE"          FLOAT,
                                                                        "ALTITUDE"           FLOAT,
                                                                        "AREA"               FLOAT,
                                                                        "ANOINST"            VARCHAR(66),
                                                                        "ANOEXT"             VARCHAR(66),
                                                                        "SUCESSOR"           VARCHAR(66));

                       /*Tabela das raças*/
                       CREATE TABLE IF NOT EXISTS {child_db}.racacor("ID"                    VARCHAR(2),
                                                                     "TIPO"                  VARCHAR(66));

                       /*Tabela de estados civis do falecido*/
                       CREATE TABLE IF NOT EXISTS {child_db}.estciv("ID"                     VARCHAR(2),
                                                                     "SITUACAO"              VARCHAR(66));

                       /*Tabela das faixas de anos de instrução*/
                       CREATE TABLE IF NOT EXISTS {child_db}.esc("ID"                        VARCHAR(2),
                                                                 "FAIXA_DE_ANOS_INSTRUCAO"   VARCHAR(66));

                       /*Tabela das escolaridades*/
                       CREATE TABLE IF NOT EXISTS {child_db}.esc2010("ID"                    VARCHAR(2),
                                                                     "ESCOLARIDADE"          VARCHAR(66));

                       /*Tabela das ocupações*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ocup("ID"                       VARCHAR(6),
                                                                  "OCUPACAO"                 VARCHAR(66));

                       /*Tabela dos municípios de residência*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunres("ID"                  VARCHAR(6),
                                                                       "MUNNOME"	         VARCHAR(66),
                                                                       "MUNNOMEX"            VARCHAR(66),
                                                                       "MUNCODDV"            VARCHAR(7),
                                                                       "OBSERV"              VARCHAR(66),
                                                                       "SITUACAO"            VARCHAR(66),
                                                                       "MUNSINP"	         VARCHAR(66),
                                                                       "MUNSIAFI"            VARCHAR(66),
                                                                       "UFCOD_ID"            VARCHAR(2),
                                                                       "AMAZONIA"            VARCHAR(66),
                                                                       "FRONTEIRA"           VARCHAR(66),
                                                                       "CAPITAL"             VARCHAR(66),
                                                                       "RSAUDE_ID"           VARCHAR(5),
                                                                       "LATITUDE"            FLOAT,
                                                                       "LONGITUDE"           FLOAT,
                                                                       "ALTITUDE"            FLOAT,
                                                                       "AREA"                FLOAT,
                                                                       "ANOINST"             VARCHAR(66),
                                                                       "ANOEXT"              VARCHAR(66),
                                                                       "SUCESSOR"            VARCHAR(66));

                       /*Tabela dos lugares de falecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.lococor("ID"                    VARCHAR(2),
                                                                     "LUGAR"                 VARCHAR(66));

                       /*Tabela dos estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codestab("ID"                   VARCHAR(7),
                                                                      "DESCESTAB"            VARCHAR(66),
                                                                      "ESFERA"               VARCHAR(66),
                                                                      "REGIME"               VARCHAR(66));

                       /*Tabela dos municípios de ocorrência*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunocor("ID"                 VARCHAR(6),
                                                                        "MUNNOME"	         VARCHAR(66),
                                                                        "MUNNOMEX"           VARCHAR(66),
                                                                        "MUNCODDV"           VARCHAR(7),
                                                                        "OBSERV"             VARCHAR(66),
                                                                        "SITUACAO"           VARCHAR(66),
                                                                        "MUNSINP"	         VARCHAR(66),
                                                                        "MUNSIAFI"           VARCHAR(66),
                                                                        "UFCOD_ID"           VARCHAR(2),
                                                                        "AMAZONIA"           VARCHAR(66),
                                                                        "FRONTEIRA"          VARCHAR(66),
                                                                        "CAPITAL"            VARCHAR(66),
                                                                        "RSAUDE_ID"          VARCHAR(5),
                                                                        "LATITUDE"           FLOAT,
                                                                        "LONGITUDE"          FLOAT,
                                                                        "ALTITUDE"           FLOAT,
                                                                        "AREA"               FLOAT,
                                                                        "ANOINST"            VARCHAR(66),
                                                                        "ANOEXT"             VARCHAR(66),
                                                                        "SUCESSOR"           VARCHAR(66));

                       /*Tabela de momentos de óbito*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpmorteoco("ID"                 VARCHAR(2),
                                                                        "EPOCA_MORTE"        VARCHAR(66));

                       /*Tabela de causas básicas da DO*/
                       CREATE TABLE IF NOT EXISTS {child_db}.causabas("ID"                   VARCHAR(4),
                                                                      "DOENCA"               VARCHAR(66));

                       /*Tabela das circunstâncias de morte violenta*/
                       CREATE TABLE IF NOT EXISTS {child_db}.circobito("ID"                  VARCHAR(2),
                                                                       "CIRCUNSTANCIA"       VARCHAR(66));

                       /*Tabela de fontes de informação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.fonte("ID"                      VARCHAR(2),
                                                                   "ORIGEM"                  VARCHAR(66));

                       /*Tabela de causas básicas originais da morte*/
                       CREATE TABLE IF NOT EXISTS {child_db}.causabas_o("ID"                 VARCHAR(4),
                                                                        "DOENCA"             VARCHAR(66));

                       /*Tabela se o atestador do falecimento é médico ou outro*/
                       CREATE TABLE IF NOT EXISTS {child_db}.atestante("ID"                  VARCHAR(2),
                                                                       "ATESTADOR"           VARCHAR(66));

                       /*Tabela de fontes de investigação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.fonteinv("ID"                   VARCHAR(2),
                                                                      "ORIGEM"               VARCHAR(66));

                       /*Tabela de escolaridades da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.escmaeagr1("ID"                 VARCHAR(2),
                                                                        "ESCOLARIDADE"       VARCHAR(66));

                       /*Tabela de escolaridades do falecido*/
                       CREATE TABLE IF NOT EXISTS {child_db}.escfalagr1("ID"                 VARCHAR(2),
                                                                        "ESCOLARIDADE"       VARCHAR(66));

                       /*Tabela de momentos de óbito*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpobitocor("ID"                 VARCHAR(2),
                                                                        "EPOCA_MORTE"        VARCHAR(66));

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                      VARCHAR(2),
                                                                   "ESTADO"                  VARCHAR(66),
                                                                   "SIGLA_UF"                VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                     VARCHAR(5),
                                                                    "REGIAO"                 VARCHAR(66));

                       /*Tabela de Arquivos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.arquivos("NOME"                 VARCHAR(15),
                                                                      "DIRETORIO"            VARCHAR(66),
                                                                      "DATA_INSERCAO_FTP"    DATE,
                                                                      "DATA_HORA_CARGA"      TIMESTAMP,
                                                                      "QTD_REGISTROS"        INTEGER);
                ''')
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()

########################################################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC  #
########################################################################################################################################################

import psycopg2


"""
Cria o schema do banco de dados do SINASC para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do banco de dados sinasc
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Declarações de Nascimento (tabela principal do banco de dados sinasc)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.dnbr("NUMERODN"                        VARCHAR(8),
                                                                  "UF_DN"                           VARCHAR(2),
                                                                  "ANO_DN"                          INTEGER,
                                                                  "CODINST"                         VARCHAR(18),
                                                                  "CODESTAB_ID"                     VARCHAR(7),
                                                                  "CODMUNNASC_ID"                   VARCHAR(6),
                                                                  "LOCNASC_ID"                      VARCHAR(2),
                                                                  "IDADEMAE"                        FLOAT,
                                                                  "ESTCIVMAE_ID"                    VARCHAR(2),
                                                                  "ESCMAE_ID"                       VARCHAR(2),
                                                                  "CODOCUPMAE_ID"                   VARCHAR(6),
                                                                  "QTDFILVIVO"                      FLOAT,
                                                                  "QTDFILMORT"                      FLOAT,
                                                                  "CODMUNRES_ID"                    VARCHAR(6),
                                                                  "GESTACAO_ID"                     VARCHAR(2),
                                                                  "GRAVIDEZ_ID"                     VARCHAR(2),
                                                                  "PARTO_ID"                        VARCHAR(2),
                                                                  "CONSULTAS_ID"                    VARCHAR(2),
                                                                  "DTNASC"                          DATE,
                                                                  "HORANASC"                        VARCHAR(4),
                                                                  "SEXO"                            VARCHAR(2),
                                                                  "APGAR1"                          FLOAT,
                                                                  "APGAR5"                          FLOAT,
                                                                  "RACACOR_ID"                      VARCHAR(2),
                                                                  "PESO"                            FLOAT,
                                                                  "IDANOMAL"                        NUMERIC,
                                                                  "DTCADASTRO"                      DATE,
                                                                  "CODANOMAL_ID"                    VARCHAR(4),
                                                                  "DTRECEBIM"                       DATE,
                                                                  "DIFDATA"                         FLOAT,
                                                                  "NATURALMAE_ID"                   VARCHAR(3),
                                                                  "CODMUNNATU_ID"                   VARCHAR(6),
                                                                  "ESCMAE2010_ID"                   VARCHAR(2),
                                                                  "DTNASCMAE"                       DATE,
                                                                  "RACACORMAE_ID"                   VARCHAR(2),
                                                                  "QTDGESTANT"                      FLOAT,
                                                                  "QTDPARTNOR"                      FLOAT,
                                                                  "QTDPARTCES"                      FLOAT,
                                                                  "IDADEPAI"                        FLOAT,
                                                                  "DTULTMENST"                      DATE,
                                                                  "SEMAGESTAC"                      FLOAT,
                                                                  "TPMETESTIM_ID"                   VARCHAR(2),
                                                                  "CONSPRENAT"                      FLOAT,
                                                                  "MESPRENAT"                       VARCHAR(2),
                                                                  "TPAPRESENT_ID"                   VARCHAR(2),
                                                                  "STTRABPART_ID"                   VARCHAR(2),
                                                                  "STCESPARTO_ID"                   VARCHAR(2),
                                                                  "TPNASCASSI_ID"                   VARCHAR(2),
                                                                  "TPFUNCRESP_ID"                   VARCHAR(2),
                                                                  "TPDOCRESP"                       VARCHAR(5),
                                                                  "DTDECLARAC"                      DATE,
                                                                  "ESCMAEAGR1_ID"                   VARCHAR(2),
                                                                  "STDNEPIDEM"                      NUMERIC,
                                                                  "STDNNOVA"                        NUMERIC,
                                                                  "CODPAISRES_ID"                   VARCHAR(3),
                                                                  "TPROBSON_ID"                     VARCHAR(2),
                                                                  "PARIDADE"                        NUMERIC,
                                                                  "KOTELCHUCK"                      VARCHAR(2));

                       /*Tabela dos estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codestab("ID"                          VARCHAR(7),
                                                                      "DESCESTAB"                   VARCHAR(66),
                                                                      "ESFERA"                      VARCHAR(66),
                                                                      "REGIME"                      VARCHAR(66));

                       /*Tabela dos municípios de nascimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunnasc("ID"                        VARCHAR(6),
                                                                        "MUNNOME"	                VARCHAR(66),
                                                                        "MUNNOMEX"                  VARCHAR(66),
                                                                        "MUNCODDV"                  VARCHAR(7),
                                                                        "OBSERV"                    VARCHAR(66),
                                                                        "SITUACAO"                  VARCHAR(66),
                                                                        "MUNSINP"	                VARCHAR(66),
                                                                        "MUNSIAFI"                  VARCHAR(66),
                                                                        "UFCOD_ID"                  VARCHAR(2),
                                                                        "AMAZONIA"                  VARCHAR(66),
                                                                        "FRONTEIRA"                 VARCHAR(66),
                                                                        "CAPITAL"                   VARCHAR(66),
                                                                        "RSAUDE_ID"                 VARCHAR(5),
                                                                        "LATITUDE"                  FLOAT,
                                                                        "LONGITUDE"                 FLOAT,
                                                                        "ALTITUDE"                  FLOAT,
                                                                        "AREA"                      FLOAT,
                                                                        "ANOINST"                   VARCHAR(66),
                                                                        "ANOEXT"                    VARCHAR(66),
                                                                        "SUCESSOR"                  VARCHAR(66));

                       /*Tabela dos locais de nascimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.locnasc("ID"                           VARCHAR(2),
                                                                     "LOCAL"                        VARCHAR(66));

                       /*Tabela de estados civis da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.estcivmae("ID"                         VARCHAR(2),
                                                                       "SITUACAO"                   VARCHAR(66));

                       /*Tabela das faixas de anos de instrução da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.escmae("ID"                            VARCHAR(2),
                                                                    "FAIXA_DE_ANOS_INSTRUCAO"       VARCHAR(66));

                       /*Tabela das ocupações da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codocupmae("ID"                        VARCHAR(6),
                                                                  "OCUPACAO"                        VARCHAR(66));

                       /*Tabela dos municípios de residência da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunres("ID"                         VARCHAR(6),
                                                                       "MUNNOME"	                VARCHAR(66),
                                                                       "MUNNOMEX"                   VARCHAR(66),
                                                                       "MUNCODDV"                   VARCHAR(7),
                                                                       "OBSERV"                     VARCHAR(66),
                                                                       "SITUACAO"                   VARCHAR(66),
                                                                       "MUNSINP"	                VARCHAR(66),
                                                                       "MUNSIAFI"                   VARCHAR(66),
                                                                       "UFCOD_ID"                   VARCHAR(2),
                                                                       "AMAZONIA"                   VARCHAR(66),
                                                                       "FRONTEIRA"                  VARCHAR(66),
                                                                       "CAPITAL"                    VARCHAR(66),
                                                                       "RSAUDE_ID"                  VARCHAR(5),
                                                                       "LATITUDE"                   FLOAT,
                                                                       "LONGITUDE"                  FLOAT,
                                                                       "ALTITUDE"                   FLOAT,
                                                                       "AREA"                       FLOAT,
                                                                       "ANOINST"                    VARCHAR(66),
                                                                       "ANOEXT"                     VARCHAR(66),
                                                                       "SUCESSOR"                   VARCHAR(66));

                       /*Tabela das faixas de semanas de gestação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.gestacao("ID"                          VARCHAR(2),
                                                                      "FAIXA_DE_SEMANAS_GESTACAO"   VARCHAR(66));

                       /*Tabela das multiplicidades da gestação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.gravidez("ID"                          VARCHAR(2),
                                                                      "MULTIPLICIDADE_GESTACAO"     VARCHAR(66));

                       /*Tabela dos tipos de parto*/
                       CREATE TABLE IF NOT EXISTS {child_db}.parto("ID"                             VARCHAR(2),
                                                                   "TIPO"                           VARCHAR(66));

                       /*Tabela das faixas do número de consultas*/
                       CREATE TABLE IF NOT EXISTS {child_db}.consultas("ID"                         VARCHAR(2),
                                                                       "FAIXA_DE_NUMERO_CONSULTAS"  VARCHAR(66));

                       /*Tabela dos tipos de raça do RN*/
                       CREATE TABLE IF NOT EXISTS {child_db}.racacor("ID"                           VARCHAR(2),
                                                                     "TIPO"                         VARCHAR(66));

                       /*Tabela dos tipos de anomalia do RN*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codanomal("ID"                         VARCHAR(4),
                                                                       "ANOMALIA"                   VARCHAR(66));

                       /*Tabela do local de nascimento da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.naturalmae("ID"                        VARCHAR(3),
                                                                        "LOCAL"                     VARCHAR(66));

                       /*Tabela dos municípios de nascimento da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codmunnatu("ID"                        VARCHAR(6),
                                                                        "MUNNOME"	                VARCHAR(66),
                                                                        "MUNNOMEX"                  VARCHAR(66),
                                                                        "MUNCODDV"                  VARCHAR(7),
                                                                        "OBSERV"                    VARCHAR(66),
                                                                        "SITUACAO"                  VARCHAR(66),
                                                                        "MUNSINP"	                VARCHAR(66),
                                                                        "MUNSIAFI"                  VARCHAR(66),
                                                                        "UFCOD_ID"                  VARCHAR(2),
                                                                        "AMAZONIA"                  VARCHAR(66),
                                                                        "FRONTEIRA"                 VARCHAR(66),
                                                                        "CAPITAL"                   VARCHAR(66),
                                                                        "RSAUDE_ID"                 VARCHAR(5),
                                                                        "LATITUDE"                  FLOAT,
                                                                        "LONGITUDE"                 FLOAT,
                                                                        "ALTITUDE"                  FLOAT,
                                                                        "AREA"                      FLOAT,
                                                                        "ANOINST"                   VARCHAR(66),
                                                                        "ANOEXT"                    VARCHAR(66),
                                                                        "SUCESSOR"                  VARCHAR(66));

                       /*Tabela das escolaridades da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.escmae2010("ID"                        VARCHAR(2),
                                                                        "ESCOLARIDADE"              VARCHAR(66));

                       /*Tabela dos tipos de raça da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.racacormae("ID"                        VARCHAR(2),
                                                                        "TIPO"                      VARCHAR(66));

                       /*Tabela dos métodos utilizados para saber o número de semanas de gestação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpmetestim("ID"                        VARCHAR(2),
                                                                        "METODO"                    VARCHAR(66));

                       /*Tabela dos tipos de posição do RN*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpapresent("ID"                        VARCHAR(2),
                                                                        "POSICAO"                   VARCHAR(66));

                       /*Tabela da indução ou não do parto*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sttrabpart("ID"                        VARCHAR(2),
                                                                        "INDUCAO"                   VARCHAR(66));

                       /*Tabela se a cesárea ocorreu ou não antes do parto iniciar*/
                       CREATE TABLE IF NOT EXISTS {child_db}.stcesparto("ID"                        VARCHAR(2),
                                                                        "CESAREA_ANTES_PARTO"       VARCHAR(66));

                       /*Tabela dos tipos de profissional que assistem parto*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpnascassi("ID"                        VARCHAR(2),
                                                                        "ASSISTENCIA"               VARCHAR(66));

                       /*Tabela dos tipos de função do responsável pelo preenchimento da DN*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpfuncresp("ID"                        VARCHAR(2),
                                                                        "FUNCAO"                    VARCHAR(66));

                       /*Tabela das escolaridades da mãe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.escmaeagr1("ID"                        VARCHAR(2),
                                                                        "ESCOLARIDADE"              VARCHAR(66));

                       /*Tabela do país de nascimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codpaisres("ID"                        VARCHAR(3),
                                                                        "PAIS"                      VARCHAR(66));

                       /*Tabela da classificação de Robson*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tprobson("ID"                          VARCHAR(2),
                                                                      "DESCRICAO"                   VARCHAR(66));

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                             VARCHAR(2),
                                                                   "ESTADO"                         VARCHAR(66),
                                                                   "SIGLA_UF"                       VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                            VARCHAR(5),
                                                                    "REGIAO"                        VARCHAR(66));

                       /*Tabela de Arquivos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.arquivos("NOME"                        VARCHAR(15),
                                                                      "DIRETORIO"                   VARCHAR(66),
                                                                      "DATA_INSERCAO_FTP"           DATE,
                                                                      "DATA_HORA_CARGA"             TIMESTAMP,
                                                                      "QTD_REGISTROS"               INTEGER);
                ''')
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()

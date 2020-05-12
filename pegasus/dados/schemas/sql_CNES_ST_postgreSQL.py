###########################################################################################################################################################
# CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
###########################################################################################################################################################

import psycopg2


"""
Cria o schema do sub banco de dados do CNES_ST (Estabelecimentos) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e Microsoft SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do sub banco de dados cnes_st
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela Estabelecimentos (tabela principal do sub banco de dados cnes_st)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.stbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_ST"                   VARCHAR(2),
                                                                  "ANO_ST"                  INTEGER,
                                                                  "MES_ST"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "COD_CEP"                 VARCHAR(8),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "PFPJ_ID"                 VARCHAR(2),
                                                                  "NIVDEP_ID"               VARCHAR(2),
                                                                  "CNPJ_MAN"                VARCHAR(14),
                                                                  "CODIR_ID"                VARCHAR(2),
                                                                  "VINC_SUS"                NUMERIC,
                                                                  "TPGESTAO_ID"             VARCHAR(2),
                                                                  "ESFERAA_ID"              VARCHAR(2),
                                                                  "RETENCAO_ID"             VARCHAR(2),
                                                                  "ATIVIDAD_ID"             VARCHAR(2),
                                                                  "NATUREZA_ID"             VARCHAR(2),
                                                                  "CLIENTEL_ID"             VARCHAR(2),
                                                                  "TPUNID_ID"               VARCHAR(2),
                                                                  "TURNOAT_ID"              VARCHAR(2),
                                                                  "NIVHIER_ID"              VARCHAR(2),
                                                                  "TPPREST_ID"              VARCHAR(2),
                                                                  "CO_BANCO"                VARCHAR(3),
                                                                  "CO_AGENC"                VARCHAR(5),
                                                                  "C_CORREN"                VARCHAR(14),
                                                                  "ALVARA"                  VARCHAR(25),
                                                                  "DT_EXPED"                DATE,
                                                                  "ORGEXPED_ID"             VARCHAR(2),
                                                                  "AV_ACRED"                NUMERIC,
                                                                  "CLASAVAL_ID"             VARCHAR(2),
                                                                  "DT_ACRED"                DATE,
                                                                  "AV_PNASS"                NUMERIC,
                                                                  "DT_PNASS"                DATE,
                                                                  "GESPRG1E"                NUMERIC,
                                                                  "GESPRG1M"                NUMERIC,
                                                                  "GESPRG2E"                NUMERIC,
                                                                  "GESPRG2M"                NUMERIC,
                                                                  "GESPRG4E"                NUMERIC,
                                                                  "GESPRG4M"                NUMERIC,
                                                                  "NIVATE_A"                NUMERIC,
                                                                  "GESPRG3E"                NUMERIC,
                                                                  "GESPRG3M"                NUMERIC,
                                                                  "GESPRG5E"                NUMERIC,
                                                                  "GESPRG5M"                NUMERIC,
                                                                  "GESPRG6E"                NUMERIC,
                                                                  "GESPRG6M"                NUMERIC,
                                                                  "NIVATE_H"                NUMERIC,
                                                                  "QTLEITP1"                FLOAT,
                                                                  "QTLEITP2"                FLOAT,
                                                                  "QTLEITP3"                FLOAT,
                                                                  "LEITHOSP"                NUMERIC,
                                                                  "QTINST01"                FLOAT,
                                                                  "QTINST02"                FLOAT,
                                                                  "QTINST03"                FLOAT,
                                                                  "QTINST04"                FLOAT,
                                                                  "QTINST05"                FLOAT,
                                                                  "QTINST06"                FLOAT,
                                                                  "QTINST07"                FLOAT,
                                                                  "QTINST08"                FLOAT,
                                                                  "QTINST09"                FLOAT,
                                                                  "QTINST10"                FLOAT,
                                                                  "QTINST11"                FLOAT,
                                                                  "QTINST12"                FLOAT,
                                                                  "QTINST13"                FLOAT,
                                                                  "QTINST14"                FLOAT,
                                                                  "URGEMERG"                NUMERIC,
                                                                  "QTINST15"                FLOAT,
                                                                  "QTINST16"                FLOAT,
                                                                  "QTINST17"                FLOAT,
                                                                  "QTINST18"                FLOAT,
                                                                  "QTINST19"                FLOAT,
                                                                  "QTINST20"                FLOAT,
                                                                  "QTINST21"                FLOAT,
                                                                  "QTINST22"                FLOAT,
                                                                  "QTINST23"                FLOAT,
                                                                  "QTINST24"                FLOAT,
                                                                  "QTINST25"                FLOAT,
                                                                  "QTINST26"                FLOAT,
                                                                  "QTINST27"                FLOAT,
                                                                  "QTINST28"                FLOAT,
                                                                  "QTINST29"                FLOAT,
                                                                  "QTINST30"                FLOAT,
                                                                  "ATENDAMB"                NUMERIC,
                                                                  "QTINST31"                FLOAT,
                                                                  "QTINST32"                FLOAT,
                                                                  "QTINST33"                FLOAT,
                                                                  "CENTRCIR"                NUMERIC,
                                                                  "QTINST34"                FLOAT,
                                                                  "QTINST35"                FLOAT,
                                                                  "QTINST36"                FLOAT,
                                                                  "QTINST37"                FLOAT,
                                                                  "CENTROBS"                NUMERIC,
                                                                  "QTLEIT05"                FLOAT,
                                                                  "QTLEIT06"                FLOAT,
                                                                  "QTLEIT07"                FLOAT,
                                                                  "QTLEIT09"                FLOAT,
                                                                  "QTLEIT19"                FLOAT,
                                                                  "QTLEIT20"                FLOAT,
                                                                  "QTLEIT21"                FLOAT,
                                                                  "QTLEIT22"                FLOAT,
                                                                  "QTLEIT23"                FLOAT,
                                                                  "QTLEIT32"                FLOAT,
                                                                  "QTLEIT34"                FLOAT,
                                                                  "QTLEIT38"                FLOAT,
                                                                  "QTLEIT39"                FLOAT,
                                                                  "QTLEIT40"                FLOAT,
                                                                  "CENTRNEO"                NUMERIC,
                                                                  "ATENDHOS"                NUMERIC,
                                                                  "SERAP01P"                NUMERIC,
                                                                  "SERAP01T"                NUMERIC,
                                                                  "SERAP02P"                NUMERIC,
                                                                  "SERAP02T"                NUMERIC,
                                                                  "SERAP03P"                NUMERIC,
                                                                  "SERAP03T"                NUMERIC,
                                                                  "SERAP04P"                NUMERIC,
                                                                  "SERAP04T"                NUMERIC,
                                                                  "SERAP05P"                NUMERIC,
                                                                  "SERAP05T"                NUMERIC,
                                                                  "SERAP06P"                NUMERIC,
                                                                  "SERAP06T"                NUMERIC,
                                                                  "SERAP07P"                NUMERIC,
                                                                  "SERAP07T"                NUMERIC,
                                                                  "SERAP08P"                NUMERIC,
                                                                  "SERAP08T"                NUMERIC,
                                                                  "SERAP09P"                NUMERIC,
                                                                  "SERAP09T"                NUMERIC,
                                                                  "SERAP10P"                NUMERIC,
                                                                  "SERAP10T"                NUMERIC,
                                                                  "SERAP11P"                NUMERIC,
                                                                  "SERAP11T"                NUMERIC,
                                                                  "SERAPOIO"                NUMERIC,
                                                                  "RES_BIOL"                NUMERIC,
                                                                  "RES_QUIM"                NUMERIC,
                                                                  "RES_RADI"                NUMERIC,
                                                                  "RES_COMU"                NUMERIC,
                                                                  "COLETRES"                NUMERIC,
                                                                  "COMISS01"                NUMERIC,
                                                                  "COMISS02"                NUMERIC,
                                                                  "COMISS03"                NUMERIC,
                                                                  "COMISS04"                NUMERIC,
                                                                  "COMISS05"                NUMERIC,
                                                                  "COMISS06"                NUMERIC,
                                                                  "COMISS07"                NUMERIC,
                                                                  "COMISS08"                NUMERIC,
                                                                  "COMISS09"                NUMERIC,
                                                                  "COMISS10"                NUMERIC,
                                                                  "COMISS11"                NUMERIC,
                                                                  "COMISS12"                NUMERIC,
                                                                  "COMISSAO"                NUMERIC,
                                                                  "AP01CV01"                NUMERIC,
                                                                  "AP01CV02"                NUMERIC,
                                                                  "AP01CV05"                NUMERIC,
                                                                  "AP01CV06"                NUMERIC,
                                                                  "AP01CV03"                NUMERIC,
                                                                  "AP01CV04"                NUMERIC,
                                                                  "AP02CV01"                NUMERIC,
                                                                  "AP02CV02"                NUMERIC,
                                                                  "AP02CV05"                NUMERIC,
                                                                  "AP02CV06"                NUMERIC,
                                                                  "AP02CV03"                NUMERIC,
                                                                  "AP02CV04"                NUMERIC,
                                                                  "AP03CV01"                NUMERIC,
                                                                  "AP03CV02"                NUMERIC,
                                                                  "AP03CV05"                NUMERIC,
                                                                  "AP03CV06"                NUMERIC,
                                                                  "AP03CV03"                NUMERIC,
                                                                  "AP03CV04"                NUMERIC,
                                                                  "AP04CV01"                NUMERIC,
                                                                  "AP04CV02"                NUMERIC,
                                                                  "AP04CV05"                NUMERIC,
                                                                  "AP04CV06"                NUMERIC,
                                                                  "AP04CV03"                NUMERIC,
                                                                  "AP04CV04"                NUMERIC,
                                                                  "AP05CV01"                NUMERIC,
                                                                  "AP05CV02"                NUMERIC,
                                                                  "AP05CV05"                NUMERIC,
                                                                  "AP05CV06"                NUMERIC,
                                                                  "AP05CV03"                NUMERIC,
                                                                  "AP05CV04"                NUMERIC,
                                                                  "AP06CV01"                NUMERIC,
                                                                  "AP06CV02"                NUMERIC,
                                                                  "AP06CV05"                NUMERIC,
                                                                  "AP06CV06"                NUMERIC,
                                                                  "AP06CV03"                NUMERIC,
                                                                  "AP06CV04"                NUMERIC,
                                                                  "AP07CV01"                NUMERIC,
                                                                  "AP07CV02"                NUMERIC,
                                                                  "AP07CV05"                NUMERIC,
                                                                  "AP07CV06"                NUMERIC,
                                                                  "AP07CV03"                NUMERIC,
                                                                  "AP07CV04"                NUMERIC,
                                                                  "ATEND_PR"                NUMERIC,
                                                                  "NATJUR_ID"               VARCHAR(4));

                       /*Tabela dos estabelecimentos de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cnes("ID"                      VARCHAR(7),
                                                                  "DESCESTAB"               VARCHAR(66),
                                                                  "RSOC_MAN"                VARCHAR(66),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "EXCLUIDO"                NUMERIC,
                                                                  "DATAINCL"                DATE,
                                                                  "DATAEXCL"                DATE);

                       /*Tabela dos municípios de localização de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codufmun("ID"                  VARCHAR(6),
                                                                      "MUNNOME"	            VARCHAR(66),
                                                                      "MUNNOMEX"            VARCHAR(66),
                                                                      "MUNCODDV"            VARCHAR(7),
                                                                      "OBSERV"              VARCHAR(66),
                                                                      "SITUACAO"            VARCHAR(66),
                                                                      "MUNSINP"	            VARCHAR(66),
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

                       /*Tabela se o estabelecimento é pessoa física ou pessoa jurídica*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pfpj("ID"                      VARCHAR(2),
                                                                  "PESSOA"                  VARCHAR(66));

                       /*Tabela do grau de independência do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.nivdep("ID"                    VARCHAR(2),
                                                                    "TIPO"                  VARCHAR(66));

                       /*Tabela dos tipos de retenção de tributos da mantenedora*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codir("ID"                     VARCHAR(2),
                                                                   "RETENCAO"               VARCHAR(66));

                       /*Tabela dos tipos de gestão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpgestao("ID"                  VARCHAR(2),
                                                                      "GESTAO"              VARCHAR(66));

                       /*Tabela dos tipos de administração*/
                       CREATE TABLE IF NOT EXISTS {child_db}.esferaa("ID"                   VARCHAR(2),
                                                                     "ADMINISTRACAO"        VARCHAR(66));

                       /*Tabela dos tipos de retenção de tributos do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.retencao("ID"                  VARCHAR(2),
                                                                      "RETENCAO"            VARCHAR(66));

                       /*Tabela dos tipos de atividade de ensino, se houver*/
                       CREATE TABLE IF NOT EXISTS {child_db}.atividad("ID"                  VARCHAR(2),
                                                                      "ATIVIDADE"           VARCHAR(66));

                       /*Tabela da natureza do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.natureza("ID"                  VARCHAR(2),
                                                                      "NATUREZA"            VARCHAR(66));

                       /*Tabela dos tipos de fluxo de clientela*/
                       CREATE TABLE IF NOT EXISTS {child_db}.clientel("ID"                  VARCHAR(2),
                                                                      "CLIENTELA"           VARCHAR(66));

                       /*Tabela dos tipos de estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpunid("ID"                    VARCHAR(2),
                                                                    "TIPO"                  VARCHAR(66));

                       /*Tabela dos turnos de funcionamento do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.turnoat("ID"                   VARCHAR(2),
                                                                     "TURNO"                VARCHAR(66));

                       /*Tabela dos níveis de atendimento do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.nivhier("ID"                   VARCHAR(2),
                                                                     "NIVEL"                VARCHAR(66));

                       /*Tabela dos tipos de prestador dos serviços hospitalares*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpprest("ID"                   VARCHAR(2),
                                                                     "PRESTADOR"            VARCHAR(66));

                       /*Tabela dos tipos de órgão expedidor de alvará*/
                       CREATE TABLE IF NOT EXISTS {child_db}.orgexped("ID"                  VARCHAR(2),
                                                                      "EXPEDIDOR"           VARCHAR(66));

                       /*Tabela das classificações de avaliacao de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.clasaval("ID"                  VARCHAR(2),
                                                                      "AVALIACAO"           VARCHAR(66));

                       /*Tabela das naturezas jurídicas de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.natjur("ID"                    VARCHAR(4),
                                                                    "NATUREZA"              VARCHAR(100));

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

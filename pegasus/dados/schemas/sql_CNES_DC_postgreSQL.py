###########################################################################################################################################################
# CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC #
###########################################################################################################################################################

import psycopg2


"""
Cria o schema do sub banco de dados do CNES_DC (Dados Complementares) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do sub banco de dados cnes_dc
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela Dados Complementares (tabela principal do sub banco de dados cnes_dc)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.dcbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_DC"                   VARCHAR(2),
                                                                  "ANO_DC"                  INTEGER,
                                                                  "MES_DC"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "S_HBSAGP"                FLOAT,
                                                                  "S_HBSAGN"                FLOAT,
                                                                  "S_DPI"                   FLOAT,
                                                                  "S_DPAC"                  FLOAT,
                                                                  "S_REAGP"                 FLOAT,
                                                                  "S_REAGN"                 FLOAT,
                                                                  "S_REHCV"                 FLOAT,
                                                                  "MAQ_PROP"                FLOAT,
                                                                  "MAQ_OUTR"                FLOAT,
                                                                  "F_AREIA"                 NUMERIC,
                                                                  "F_CARVAO"                NUMERIC,
                                                                  "ABRANDAD"                NUMERIC,
                                                                  "DEIONIZA"                NUMERIC,
                                                                  "OSMOSE_R"                NUMERIC,
                                                                  "OUT_TRAT"                NUMERIC,
                                                                  "CNS_NEFR"                VARCHAR(15),
                                                                  "DIALISE"                 NUMERIC,
                                                                  "SIMUL_RD"                FLOAT,
                                                                  "PLANJ_RD"                FLOAT,
                                                                  "ARMAZ_FT"                FLOAT,
                                                                  "CONF_MAS"                FLOAT,
                                                                  "SALA_MOL"                FLOAT,
                                                                  "BLOCOPER"                FLOAT,
                                                                  "S_ARMAZE"                FLOAT,
                                                                  "S_PREPAR"                FLOAT,
                                                                  "S_QCDURA"                FLOAT,
                                                                  "S_QLDURA"                FLOAT,
                                                                  "S_CPFLUX"                FLOAT,
                                                                  "S_SIMULA"                FLOAT,
                                                                  "S_ACELL6"                FLOAT,
                                                                  "S_ALSEME"                FLOAT,
                                                                  "S_ALCOME"                FLOAT,
                                                                  "ORTV1050"                FLOAT,
                                                                  "ORV50150"                FLOAT,
                                                                  "OV150500"                FLOAT,
                                                                  "UN_COBAL"                FLOAT,
                                                                  "EQBRBAIX"                FLOAT,
                                                                  "EQBRMEDI"                FLOAT,
                                                                  "EQBRALTA"                FLOAT,
                                                                  "EQ_MAREA"                FLOAT,
                                                                  "EQ_MINDI"                FLOAT,
                                                                  "EQSISPLN"                FLOAT,
                                                                  "EQDOSCLI"                FLOAT,
                                                                  "EQFONSEL"                FLOAT,
                                                                  "CNS_ADM"                 VARCHAR(15),
                                                                  "CNS_OPED"                VARCHAR(15),
                                                                  "CNS_CONC"                VARCHAR(15),
                                                                  "CNS_OCLIN"               VARCHAR(15),
                                                                  "CNS_MRAD"                VARCHAR(15),
                                                                  "CNS_FNUC"                VARCHAR(15),
                                                                  "QUIMRADI"                NUMERIC,
                                                                  "S_RECEPC"                FLOAT,
                                                                  "S_TRIHMT"                FLOAT,
                                                                  "S_TRICLI"                FLOAT,
                                                                  "S_COLETA"                FLOAT,
                                                                  "S_AFERES"                FLOAT,
                                                                  "S_PREEST"                FLOAT,
                                                                  "S_PROCES"                FLOAT,
                                                                  "S_ESTOQU"                FLOAT,
                                                                  "S_DISTRI"                FLOAT,
                                                                  "S_SOROLO"                FLOAT,
                                                                  "S_IMUNOH"                FLOAT,
                                                                  "S_PRETRA"                FLOAT,
                                                                  "S_HEMOST"                FLOAT,
                                                                  "S_CONTRQ"                FLOAT,
                                                                  "S_BIOMOL"                FLOAT,
                                                                  "S_IMUNFE"                FLOAT,
                                                                  "S_TRANSF"                FLOAT,
                                                                  "S_SGDOAD"                FLOAT,
                                                                  "QT_CADRE"                FLOAT,
                                                                  "QT_CENRE"                FLOAT,
                                                                  "QT_REFSA"                FLOAT,
                                                                  "QT_CONRA"                FLOAT,
                                                                  "QT_EXTPL"                FLOAT,
                                                                  "QT_FRE18"                FLOAT,
                                                                  "QT_FRE30"                FLOAT,
                                                                  "QT_AGIPL"                FLOAT,
                                                                  "QT_SELAD"                FLOAT,
                                                                  "QT_IRRHE"                FLOAT,
                                                                  "QT_AGLTN"                FLOAT,
                                                                  "QT_MAQAF"                FLOAT,
                                                                  "QT_REFRE"                FLOAT,
                                                                  "QT_REFAS"                FLOAT,
                                                                  "QT_CAPFL"                FLOAT,
                                                                  "CNS_HMTR"                VARCHAR(15),
                                                                  "CNS_HMTL"                VARCHAR(15),
                                                                  "CNS_CRES"                VARCHAR(15),
                                                                  "CNS_RTEC"                VARCHAR(15),
                                                                  "HEMOTERA"                NUMERIC);

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

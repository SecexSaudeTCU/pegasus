###########################################################################################################################################################################
# SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP SIH_SP #
###########################################################################################################################################################################

import psycopg2


"""
Cria o schema do sub banco de dados do SIH_SP (AIH Serviços Profissionais) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do sub banco de dados sih_sp
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela das AIH Serviços Profissionais (SP) (tabela principal do sub banco de dados SIH_SP)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spbr("SP_NAIH"                 VARCHAR(13),
                                                                  "UF_SP"                   VARCHAR(2),
                                                                  "ANO_SP"                  INTEGER,
                                                                  "MES_SP"                  VARCHAR(2),
                                                                  "SPPROCREA_ID"            VARCHAR(10),
                                                                  "SPGESTOR_ID"             VARCHAR(6),
                                                                  "SPCNES_ID"               VARCHAR(7),
                                                                  "SP_DTINTER"              DATE,
                                                                  "SP_DTSAIDA"              DATE,
                                                                  "SP_CPFCGC"               VARCHAR(14),
                                                                  "SPATOPROF_ID"            VARCHAR(10),
                                                                  "SP_QTD_ATO"              FLOAT,
                                                                  "SP_PTSP"                 FLOAT,
                                                                  "SP_VALATO"               FLOAT,
                                                                  "SPMHOSP_ID"              VARCHAR(6),
                                                                  "SPMPAC_ID"               VARCHAR(6),
                                                                  "SP_DES_HOS"              NUMERIC,
                                                                  "SP_DES_PAC"              NUMERIC,
                                                                  "SPCOMPLEX_ID"            VARCHAR(2),
                                                                  "SPFINANC_ID"             VARCHAR(2),
                                                                  "SPCOFAEC_ID"             VARCHAR(6),
                                                                  "SPPFCBO_ID"              VARCHAR(6),
                                                                  "SP_PF_DOC"               VARCHAR(11),
                                                                  "SP_PJ_DOC"               VARCHAR(7),
                                                                  "INTPVAL_ID"              VARCHAR(2),
                                                                  "SERVCLA_ID"              VARCHAR(6),
                                                                  "SPCIDPRI_ID"             VARCHAR(4),
                                                                  "SPCIDSEC_ID"             VARCHAR(4),
                                                                  "SP_QT_PROC"              FLOAT,
                                                                  "SP_U_AIH"                NUMERIC,
                                                                  "GRUPO_ID"                VARCHAR(2),
                                                                  "SUBGRUPO_ID"             VARCHAR(4),
                                                                  "FORMA_ID"                VARCHAR(6));

                       /*Tabela dos tipos de procedimento realizado*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spprocrea("ID"                 VARCHAR(10),
                                                                       "PROCEDIMENTO"       VARCHAR(100));

                       /*Tabela dos municípios gestores*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spgestor("ID"                  VARCHAR(6),
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

                       /*Tabela dos estabelecimentos de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spcnes("ID"                    VARCHAR(7),
                                                                    "DESCESTAB"             VARCHAR(66));

                       /*Tabela dos tipos de procedimento referente a ato profissional*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spatoprof("ID"                 VARCHAR(10),
                                                                       "PROCEDIMENTO"       VARCHAR(100));

                       /*Tabela dos municípios de localização do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spmhosp("ID"                   VARCHAR(6),
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

                       /*Tabela dos municípios de residência do paciente*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spmpac("ID"                    VARCHAR(6),
                                                                    "MUNNOME"	            VARCHAR(66),
                                                                    "MUNNOMEX"              VARCHAR(66),
                                                                    "MUNCODDV"              VARCHAR(7),
                                                                    "OBSERV"                VARCHAR(66),
                                                                    "SITUACAO"              VARCHAR(66),
                                                                    "MUNSINP"	            VARCHAR(66),
                                                                    "MUNSIAFI"              VARCHAR(66),
                                                                    "UFCOD_ID"              VARCHAR(2),
                                                                    "AMAZONIA"              VARCHAR(66),
                                                                    "FRONTEIRA"             VARCHAR(66),
                                                                    "CAPITAL"               VARCHAR(66),
                                                                    "RSAUDE_ID"             VARCHAR(5),
                                                                    "LATITUDE"              FLOAT,
                                                                    "LONGITUDE"             FLOAT,
                                                                    "ALTITUDE"              FLOAT,
                                                                    "AREA"                  FLOAT,
                                                                    "ANOINST"               VARCHAR(66),
                                                                    "ANOEXT"                VARCHAR(66),
                                                                    "SUCESSOR"              VARCHAR(66));

                       /*Tabela dos níveis de complexidade do ato profissional*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spcomplex("ID"                 VARCHAR(2),
                                                                       "COMPLEXIDADE"       VARCHAR(66));

                       /*Tabela dos tipos de recursos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spfinanc("ID"                  VARCHAR(2),
                                                                      "FONTE"               VARCHAR(66));

                       /*Tabela dos tipos de recursos FAEC*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spcofaec("ID"                  VARCHAR(6),
                                                                      "SUBFONTE"            VARCHAR(66));

                       /*Tabela das ocupações dos profissionais*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sppfcbo("ID"                   VARCHAR(6),
                                                                     "OCUPACAO"             VARCHAR(66));

                       /*Tabela dos tipos de recursos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.intpval("ID"                   VARCHAR(2),
                                                                     "TIPO_VALOR"           VARCHAR(66));

                       /*Tabela de classificações dos serviços*/
                       CREATE TABLE IF NOT EXISTS {child_db}.servcla("ID"                   VARCHAR(6),
                                                                     "CLASSIFICACAO"        VARCHAR(100));

                       /*Tabela dos tipos de diagnóstico principal*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spcidpri("ID"                  VARCHAR(4),
                                                                      "DIAGNOSTICO"         VARCHAR(66));

                       /*Tabela dos tipos de diagnóstico secundário*/
                       CREATE TABLE IF NOT EXISTS {child_db}.spcidsec("ID"                  VARCHAR(4),
                                                                      "DIAGNOSTICO"         VARCHAR(66));

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                     VARCHAR(2),
                                                                   "ESTADO"                 VARCHAR(66),
                                                                   "SIGLA_UF"               VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                    VARCHAR(5),
                                                                    "REGIAO"                VARCHAR(66));

                       /*Tabela dos grupos de procedimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.grupo("ID"                     VARCHAR(2),
                                                                   "GRUPO"                  VARCHAR(50));

                       /*Tabela dos subgrupos de procedimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.subgrupo("ID"                  VARCHAR(4),
                                                                      "SUBGRUPO"            VARCHAR(100));

                       /*Tabela das formas de procedimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.forma("ID"                     VARCHAR(6),
                                                                   "FORMA"                  VARCHAR(100));

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

###########################################################################################################################################################################
# SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH #
###########################################################################################################################################################################

import psycopg2


"""
Cria o schema do banco de dados do SIH para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Função de criação das tabelas do banco de dados sih
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabelas comuns a todos os sub bancos de dados do SIH*/

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                     VARCHAR(2),
                                                                   "ESTADO"                 VARCHAR(66),
                                                                   "SIGLA_UF"               VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                    VARCHAR(5),
                                                                    "REGIAO"                VARCHAR(66));

                       /*Tabela dos municípios gestores*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufzi("ID"                      VARCHAR(6),
                                                                  "MUNNOME"	                VARCHAR(66),
                                                                  "MUNNOMEX"                VARCHAR(66),
                                                                  "MUNCODDV"                VARCHAR(7),
                                                                  "OBSERV"                  VARCHAR(66),
                                                                  "SITUACAO"                VARCHAR(66),
                                                                  "MUNSINP"	                VARCHAR(66),
                                                                  "MUNSIAFI"                VARCHAR(66),
                                                                  "UFCOD_ID"                VARCHAR(2),
                                                                  "AMAZONIA"                VARCHAR(66),
                                                                  "FRONTEIRA"               VARCHAR(66),
                                                                  "CAPITAL"                 VARCHAR(66),
                                                                  "RSAUDE_ID"               VARCHAR(5),
                                                                  "LATITUDE"                FLOAT,
                                                                  "LONGITUDE"               FLOAT,
                                                                  "ALTITUDE"                FLOAT,
                                                                  "AREA"                    FLOAT,
                                                                  "ANOINST"                 VARCHAR(66),
                                                                  "ANOEXT"                  VARCHAR(66),
                                                                  "SUCESSOR"                VARCHAR(66));

                       /*Tabela dos tipos de procedimento solicitado*/
                       CREATE TABLE IF NOT EXISTS {child_db}.procsolic("ID"                 VARCHAR(10),
                                                                       "PROCEDIMENTO"       VARCHAR(100));

                       /*Tabela dos tipos de diagnóstico principal*/
                       CREATE TABLE IF NOT EXISTS {child_db}.diagprinc("ID"                 VARCHAR(4),
                                                                       "DIAGNOSTICO"        VARCHAR(66));

                       /*Tabela das ocupações dos pacientes*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cbor("ID"                      VARCHAR(6),
                                                                  "OCUPACAO"                VARCHAR(66));

                       /*Tabela dos estabelecimentos de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cnes("ID"                      VARCHAR(7),
                                                                  "DESCESTAB"               VARCHAR(66));

                        /*Tabela dos níveis de complexidade de atendimento*/
                        CREATE TABLE IF NOT EXISTS {child_db}.complex("ID"                  VARCHAR(2),
                                                                      "COMPLEXIDADE"        VARCHAR(66));

                       /*Tabela dos tipos de recursos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.financ("ID"                    VARCHAR(2),
                                                                    "FONTE"                 VARCHAR(66));

                       /*Tabela dos tipos de recursos FAEC*/
                       CREATE TABLE IF NOT EXISTS {child_db}.faectp("ID"                    VARCHAR(6),
                                                                    "SUBFONTE"              VARCHAR(66));

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

                       /*Tabelas únicas do sub bancos de dados sih_rd*/

                       /*Tabela das AIH Reduzidas (RD) (tabela principal do "sub" banco de dados SIH_RD)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rdbr("N_AIH"                   VARCHAR(13),
                                                                  "UF_RD"                   VARCHAR(2),
                                                                  "ANO_RD"                  INTEGER,
                                                                  "MES_RD"                  VARCHAR(2),
                                                                  "IDENT_ID"                VARCHAR(2),
                                                                  "UFZI_ID"                 VARCHAR(6),
                                                                  "ANO_CMPT"                INTEGER,
                                                                  "MES_CMPT"                VARCHAR(2),
                                                                  "ESPEC_ID"                VARCHAR(2),
                                                                  "CGC_HOSP"                VARCHAR(14),
                                                                  "CEP"                     VARCHAR(8),
                                                                  "MUNICRES_ID"             VARCHAR(6),
                                                                  "NASC"                    DATE,
                                                                  "SEXO"                    VARCHAR(2),
                                                                  "UTI_MES_TO"              FLOAT,
                                                                  "MARCAUTI_ID"             VARCHAR(2),
                                                                  "UTI_INT_TO"              FLOAT,
                                                                  "DIAR_ACOM"               FLOAT,
                                                                  "QT_DIARIAS"              FLOAT,
                                                                  "PROCSOLIC_ID"            VARCHAR(10),
                                                                  "PROCREA_ID"              VARCHAR(10),
                                                                  "VAL_SH"                  FLOAT,
                                                                  "VAL_SP"                  FLOAT,
                                                                  "VAL_TOT"                 FLOAT,
                                                                  "VAL_UTI"                 FLOAT,
                                                                  "US_TOT"                  FLOAT,
                                                                  "DI_INTER"                DATE,
                                                                  "DT_SAIDA"                DATE,
                                                                  "DIAGPRINC_ID"            VARCHAR(4),
                                                                  "COBRANCA_ID"             VARCHAR(4),
                                                                  "NATUREZA_ID"             VARCHAR(4),
                                                                  "NATJUR_ID"               VARCHAR(4),
                                                                  "GESTAO_ID"               VARCHAR(4),
                                                                  "IND_VDRL"                NUMERIC,
                                                                  "MUNICMOV_ID"             VARCHAR(6),
                                                                  "IDADE"                   FLOAT,
                                                                  "DIAS_PERM"               FLOAT,
                                                                  "MORTE"                   NUMERIC,
                                                                  "NACIONAL_ID"             VARCHAR(3),
                                                                  "CARINT_ID"               VARCHAR(2),
                                                                  "HOMONIMO"                VARCHAR(2),
                                                                  "NUM_FILHOS"              FLOAT,
                                                                  "INSTRU_ID"               VARCHAR(2),
                                                                  "CID_NOTIF"               VARCHAR(4),
                                                                  "CONTRACEP1_ID"           VARCHAR(2),
                                                                  "CONTRACEP2_ID"           VARCHAR(2),
                                                                  "GESTRISCO"               NUMERIC,
                                                                  "INSC_PN"                 VARCHAR(12),
                                                                  "CBOR_ID"                 VARCHAR(6),
                                                                  "CNAER_ID"                VARCHAR(3),
                                                                  "VINCPREV_ID"             VARCHAR(2),
                                                                  "GESTOR_TP"               VARCHAR(2),
                                                                  "GESTOR_CPF"              VARCHAR(11),
                                                                  "CNES_ID"                 VARCHAR(7),
                                                                  "CNPJ_MANT"               VARCHAR(14),
                                                                  "INFEHOSP"                NUMERIC,
                                                                  "CID_ASSO"                VARCHAR(4),
                                                                  "CID_MORTE"               VARCHAR(4),
                                                                  "COMPLEX_ID"              VARCHAR(2),
                                                                  "FINANC_ID"               VARCHAR(2),
                                                                  "FAECTP_ID"               VARCHAR(6),
                                                                  "REGCT_ID"                VARCHAR(4),
                                                                  "RACACOR_ID"              VARCHAR(2),
                                                                  "ETNIA_ID"                VARCHAR(4),
                                                                  "AUD_JUST"                VARCHAR(100),
                                                                  "SIS_JUST"                VARCHAR(100),
                                                                  "VAL_SH_FED"              FLOAT,
                                                                  "VAL_SP_FED"              FLOAT,
                                                                  "VAL_SH_GES"              FLOAT,
                                                                  "VAL_SP_GES"              FLOAT,
                                                                  "GRUPO_ID"                VARCHAR(2),
                                                                  "SUBGRUPO_ID"             VARCHAR(4),
                                                                  "FORMA_ID"                VARCHAR(6));

                       /*Tabela dos tipos de AIH*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ident("ID"                     VARCHAR(2),
                                                                   "TIPO_AIH"               VARCHAR(66));

                       /*Tabela dos tipos de leito*/
                       CREATE TABLE IF NOT EXISTS {child_db}.espec("ID"                     VARCHAR(2),
                                                                   "LEITO"                  VARCHAR(66));

                       /*Tabela dos tipos de UTI*/
                       CREATE TABLE IF NOT EXISTS {child_db}.marcauti("ID"                  VARCHAR(2),
                                                                      "TIPO_UTI"            VARCHAR(66));

                       /*Tabela dos motivos de saída/permanência*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cobranca("ID"                   VARCHAR(2),
                                                                      "MOTIVO"               VARCHAR(66));

                       /*Tabela dos tipos de natureza jurídica dos hospitais*/
                       CREATE TABLE IF NOT EXISTS {child_db}.natureza("ID"                   VARCHAR(2),
                                                                      "NATUREZA"             VARCHAR(66));

                       /*Tabela das naturezas jurídicas de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.natjur("ID"                     VARCHAR(4),
                                                                    "NATUREZA"               VARCHAR(100));

                       /*Tabela das naturezas jurídicas de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.gestao("ID"                     VARCHAR(2),
                                                                    "GESTAO"                 VARCHAR(66));

                       /*Tabela das nacionalidades dos pacientes*/
                       CREATE TABLE IF NOT EXISTS {child_db}.nacional("ID"                  VARCHAR(3),
                                                                      "NACIONALIDADE"       VARCHAR(66));

                       /*Tabela dos motivos de internação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.carint("ID"                    VARCHAR(2),
                                                                    "MOTIVO"                VARCHAR(66));

                       /*Tabela dos níveis de instrução*/
                       CREATE TABLE IF NOT EXISTS {child_db}.instru("ID"                    VARCHAR(2),
                                                                    "NIVEL"                 VARCHAR(66));

                       /*Tabela dos tipos de contraceptivo (primeiro)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.contracep1("ID"                VARCHAR(2),
                                                                        "CONTRACEPTIVO"     VARCHAR(66));

                       /*Tabela das atividades econômicas*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cnaer("ID"                     VARCHAR(3),
                                                                   "ATIVIDADE"              VARCHAR(66));

                       /*Tabela dos tipos de vínculo previdenciário do paciente*/
                       CREATE TABLE IF NOT EXISTS {child_db}.vincprev("ID"                  VARCHAR(2),
                                                                      "VINCULO"             VARCHAR(66));

                       /*Tabela dos tipos de regras contratuais*/
                       CREATE TABLE IF NOT EXISTS {child_db}.regct("ID"                     VARCHAR(4),
                                                                   "REGRA"                  VARCHAR(66));

                       /*Tabela dos tipos de raça do paciente*/
                       CREATE TABLE IF NOT EXISTS {child_db}.racacor("ID"                   VARCHAR(2),
                                                                     "RACA"                 VARCHAR(66));

                       /*Tabela dos tipos de etnia indígena*/
                       CREATE TABLE IF NOT EXISTS {child_db}.etnia("ID"                     VARCHAR(4),
                                                                    "INDIGENA"              VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados sih_sp*/

                       /*Tabela das AIH Serviços Profissionais (SP) (tabela principal do "sub" banco de dados SIH_SP)*/
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

                       /*Tabela dos tipos de recursos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.intpval("ID"                   VARCHAR(2),
                                                                     "TIPO_VALOR"           VARCHAR(66));

                       /*Tabela de classificações dos serviços*/
                       CREATE TABLE IF NOT EXISTS {child_db}.servcla("ID"                   VARCHAR(6),
                                                                     "CLASSIFICACAO"        VARCHAR(100));
                ''')
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()

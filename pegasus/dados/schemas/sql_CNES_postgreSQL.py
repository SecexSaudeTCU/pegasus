###########################################################################################################################################################################
#  CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES   #
###########################################################################################################################################################################

import psycopg2


"""
Cria o schema do banco de dados do CNES para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Função de criação das tabelas do banco de dados cnes
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabelas comuns a mais de um sub banco de dados do CNES*/

                       /*Tabela dos estabelecimentos de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cnes("ID"                      VARCHAR(7),
                                                                  "DESCESTAB"               VARCHAR(66),
                                                                  "RSOC_MAN"                VARCHAR(66),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "EXCLUIDO"                NUMERIC,
                                                                  "DATAINCL"                DATE,
                                                                  "DATAEXCL"                DATE);

                       /*Tabela dos Estados da RFB*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ufcod("ID"                     VARCHAR(2),
                                                                   "ESTADO"                 VARCHAR(66),
                                                                   "SIGLA_UF"               VARCHAR(66));

                       /*Tabela de regiões de saúde IBGE*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rsaude("ID"                    VARCHAR(5),
                                                                    "REGIAO"                VARCHAR(66));

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

                       /*Tabela das naturezas jurídicas de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.natjur("ID"                    VARCHAR(4),
                                                                    "NATUREZA"              VARCHAR(100));

                       /*Tabela de Arquivos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.arquivos("NOME"                VARCHAR(15),
                                                                      "DIRETORIO"           VARCHAR(66),
                                                                      "DATA_INSERCAO_FTP"   DATE,
                                                                      "DATA_HORA_CARGA"     TIMESTAMP,
                                                                      "QTD_REGISTROS"       INTEGER);

                       /*Tabelas únicas do sub bancos de dados cnes_st*/

                       /*Tabela Estabelecimentos (tabela principal do sub banco de dados cnes_st)*/
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

                       /*Tabela dos tipos de retenção de tributos da mantenedora*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codir("ID"                     VARCHAR(2),
                                                                   "RETENCAO"               VARCHAR(66));

                       /*Tabela dos tipos de órgão expedidor de alvará*/
                       CREATE TABLE IF NOT EXISTS {child_db}.orgexped("ID"                  VARCHAR(2),
                                                                      "EXPEDIDOR"           VARCHAR(66));

                       /*Tabela das classificações de avaliacao de estabelecimentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.clasaval("ID"                  VARCHAR(2),
                                                                      "AVALIACAO"           VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_dc*/

                       /*Tabela Dados Complementares (tabela principal do sub banco de dados cnes_dc)*/
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

                       /*Tabelas únicas do sub bancos de dados cnes_pf*/

                       /*Tabela Profissionais (tabela principal do sub banco de dados cnes_pf)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pfbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_PF"                   VARCHAR(2),
                                                                  "ANO_PF"                  INTEGER,
                                                                  "MES_PF"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "CBO_ID"                  VARCHAR(6),
                                                                  "CBOUNICO_ID"             VARCHAR(6),
                                                                  "NOMEPROF"                VARCHAR(60),
                                                                  "CNS_PROF"                VARCHAR(15),
                                                                  "CONSELHO_ID"             VARCHAR(2),
                                                                  "REGISTRO"                VARCHAR(13),
                                                                  "VINCULAC_ID"             VARCHAR(6),
                                                                  "VINCUL_C"                NUMERIC,
                                                                  "VINCUL_A"                NUMERIC,
                                                                  "VINCUL_N"                NUMERIC,
                                                                  "PROF_SUS"                NUMERIC,
                                                                  "PROFNSUS"                NUMERIC,
                                                                  "HORAOUTR"                FLOAT,
                                                                  "HORAHOSP"                FLOAT,
                                                                  "HORA_AMB"                FLOAT);

                       /*Tabela das especialidades dos profissionais*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cbo("ID"                       VARCHAR(6),
                                                                 "OCUPACAO"                 VARCHAR(66));

                       /*Em verdade é a mesma coisa da tabela anterior*/
                       CREATE TABLE IF NOT EXISTS {child_db}.cbounico("ID"                  VARCHAR(6),
                                                                      "OCUPACAO"            VARCHAR(66));

                       /*Tabeça dos conselhos de profissão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.conselho("ID"                  VARCHAR(2),
                                                                      "DENOMINACAO"         VARCHAR(66));

                       /*Tabeça dos tipos de vínculos do profissional*/
                       CREATE TABLE IF NOT EXISTS {child_db}.vinculac("ID"                  VARCHAR(6),
                                                                      "DESCRICAO"           VARCHAR(100));

                       /*Tabelas únicas do sub bancos de dados cnes_lt*/

                       /*Tabela Leitos (tabela principal do sub banco de dados cnes_lt)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.ltbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_LT"                   VARCHAR(2),
                                                                  "ANO_LT"                  INTEGER,
                                                                  "MES_LT"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TPLEITO_ID"              VARCHAR(2),
                                                                  "CODLEITO_ID"             VARCHAR(2),
                                                                  "QT_EXIST"                FLOAT,
                                                                  "QT_CONTR"                FLOAT,
                                                                  "QT_SUS"                  FLOAT);

                       /*Tabela dos tipos de leito do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpleito("ID"                   VARCHAR(2),
                                                                     "TIPO"                 VARCHAR(66));

                       /*Tabela dos tipos específicos dos leitos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codleito("ID"                  VARCHAR(2),
                                                                      "ESPECIALIDADE"       VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_eq*/

                       /*Tabela Equipamentos (tabela principal do sub banco de dados cnes_eq)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.eqbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_EQ"                   VARCHAR(2),
                                                                  "ANO_EQ"                  INTEGER,
                                                                  "MES_EQ"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TIPEQUIP_ID"             VARCHAR(2),
                                                                  "CODEQUIP_ID"             VARCHAR(2),
                                                                  "QT_EXIST"                FLOAT,
                                                                  "QT_USO"                  FLOAT,
                                                                  "IND_SUS"                 NUMERIC,
                                                                  "ND_NSUS"                 NUMERIC);

                       /*Tabela dos tipos de equipamentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tipequip("ID"                  VARCHAR(2),
                                                                      "TIPO"                VARCHAR(66));

                       /*Tabela dos nomes dos equipamentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.codequip("ID"                  VARCHAR(2),
                                                                      "DENOMINACAO"         VARCHAR(66));


                       /*Tabelas únicas do sub bancos de dados cnes_sr*/

                       /*Tabela Serviços Especializados (tabela principal do sub banco de dados cnes_sr)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.srbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_SR"                   VARCHAR(2),
                                                                  "ANO_SR"                  INTEGER,
                                                                  "MES_SR"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "SERVESP_ID"              VARCHAR(3),
                                                                  "CLASSSR_ID"              VARCHAR(6),
                                                                  "SRVUNICO_ID"             VARCHAR(3),
                                                                  "TPGESTAO_ID"             VARCHAR(2),
                                                                  "PFPJ_ID"                 VARCHAR(2),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "NIVDEP_ID"               VARCHAR(2),
                                                                  "ESFERAA_ID"              VARCHAR(2),
                                                                  "ATIVIDAD_ID"             VARCHAR(2),
                                                                  "RETENCAO_ID"             VARCHAR(2),
                                                                  "NATUREZA_ID"             VARCHAR(2),
                                                                  "CLIENTEL_ID"             VARCHAR(2),
                                                                  "TPUNID_ID"               VARCHAR(2),
                                                                  "TURNOAT_ID"              VARCHAR(2),
                                                                  "NIVHIER_ID"              VARCHAR(2),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "CNPJ_MAN"                VARCHAR(14),
                                                                  "CARACTER_ID"             VARCHAR(2),
                                                                  "AMB_NSUS"                NUMERIC,
                                                                  "AMB_SUS"                 NUMERIC,
                                                                  "HOSP_NSUS"               NUMERIC,
                                                                  "HOSP_SUS"                NUMERIC,
                                                                  "CONTSRVU"                NUMERIC,
                                                                  "CNESTERC"                VARCHAR(7),
                                                                  "NATJUR_ID"               VARCHAR(4));

                       /*Tabela dos serviços especializados*/
                       CREATE TABLE IF NOT EXISTS {child_db}.servesp("ID"                   VARCHAR(3),
                                                                     "DESCRICAO"            VARCHAR(100));

                       /*Tabela de classificações dos serviços especializados*/
                       CREATE TABLE IF NOT EXISTS {child_db}.classsr("ID"                   VARCHAR(6),
                                                                     "DESCRICAO"            VARCHAR(100));

                       /*Na verdade é a mesma coisa da tabela servesp*/
                       CREATE TABLE IF NOT EXISTS {child_db}.srvunico("ID"                  VARCHAR(3),
                                                                      "DESCRICAO"           VARCHAR(100));

                       /*Tabela de caracterizações do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.caracter("ID"                  VARCHAR(2),
                                                                      "CARACTERIZACAO"      VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_ep*/

                       /*Tabela Equipes (tabela principal do sub banco de dados cnes_ep)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.epbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_EP"                   VARCHAR(2),
                                                                  "ANO_EP"                  INTEGER,
                                                                  "MES_EP"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "IDEQUIPE_ID"             VARCHAR(18),
                                                                  "TIPOEQP_ID"              VARCHAR(2),
                                                                  "NOME_EQP"                VARCHAR(60),
                                                                  "IDAREA_ID"               VARCHAR(10),
                                                                  "NOMEAREA"                VARCHAR(60),
                                                                  "IDSEGM_ID"               VARCHAR(8),
                                                                  "DESCSEGM"                VARCHAR(60),
                                                                  "TIPOSEGM_ID"             VARCHAR(2),
                                                                  "DT_ATIVA"                DATE,
                                                                  "DT_DESAT"                DATE,
                                                                  "QUILOMBO"                NUMERIC,
                                                                  "ASSENTAD"                NUMERIC,
                                                                  "POPGERAL"                NUMERIC,
                                                                  "ESCOLA"                  NUMERIC,
                                                                  "INDIGENA"                NUMERIC,
                                                                  "PRONASCI"                NUMERIC,
                                                                  "MOTDESAT_ID"             VARCHAR(2),
                                                                  "TPDESAT_ID"              VARCHAR(2));

                       /*Tabelas da identificação de equipes*/
                       CREATE TABLE IF NOT EXISTS {child_db}.idequipe("ID"                  VARCHAR(18),
                                                                      "NOME_EQUIPE"         VARCHAR(66));

                       /*Tabela dos tipos de equipe*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tipoeqp("ID"                   VARCHAR(2),
                                                                     "TIPO"                 VARCHAR(66));

                       /*Tabela da identificação de áreas*/
                       CREATE TABLE IF NOT EXISTS {child_db}.idarea("ID"                    VARCHAR(10),
                                                                    "NOME_AREA"             VARCHAR(66));

                       /*Tabela da identificação de segmentos*/
                       CREATE TABLE IF NOT EXISTS {child_db}.idsegm("ID"                    VARCHAR(8),
                                                                    "DESCRICAO"             VARCHAR(66));

                       /*Tabela dos tipos de segmento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tiposegm("ID"                  VARCHAR(2),
                                                                      "TIPO"                VARCHAR(66));

                       /*Tabela dos motivos de desativação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.motdesat("ID"                  VARCHAR(2),
                                                                      "MOTIVO"              VARCHAR(66));

                       /*Tabela dos tipos de desativação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpdesat("ID"                   VARCHAR(2),
                                                                     "TIPO"                 VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_hb*/

                       /*Tabela Habilitações (tabela principal do sub banco de dados cnes_hb)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.hbbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_HB"                   VARCHAR(2),
                                                                  "ANO_HB"                  INTEGER,
                                                                  "MES_HB"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "SGRUPHAB_ID"             VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE,
                                                                  "NULEITOS"                FLOAT);

                       /*Tabela dos tipos de habilitação*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sgruphab_hb("ID"               VARCHAR(4),
                                                                         "HABILITACAO"      VARCHAR(100));

                       /*Tabelas únicas do sub bancos de dados cnes_rc*/

                       /*Tabela Regras Contratuais (tabela principal do sub banco de dados cnes_rc)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.rcbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_RC"                   VARCHAR(2),
                                                                  "ANO_RC"                  INTEGER,
                                                                  "MES_RC"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "SGRUPHAB_ID"             VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE);

                       /*Tabela dos tipos de regra contratual*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sgruphab_rc("ID"               VARCHAR(4),
                                                                         "REGRA"            VARCHAR(150));

                       /*Tabelas únicas do sub bancos de dados cnes_gm*/

                       /*Tabela Gestão e Metas (tabela principal do sub banco de dados cnes_gm)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.gmbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_GM"                   VARCHAR(2),
                                                                  "ANO_GM"                  INTEGER,
                                                                  "MES_GM"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "SGRUPHAB_ID"             VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE);

                       /*Tabela dos tipos de gestão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sgruphab_gm("ID"               VARCHAR(4),
                                                                         "GESTAO"           VARCHAR(100));

                       /*Tabelas únicas do sub bancos de dados cnes_ee*/

                       /*Tabela Estabelecimentos de Ensino (tabela principal do sub banco de dados cnes_ee)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.eebr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_EE"                   VARCHAR(2),
                                                                  "ANO_EE"                  INTEGER,
                                                                  "MES_EE"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TPGESTAO_ID"             VARCHAR(2),
                                                                  "PFPJ_ID"                 VARCHAR(2),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "NIVDEP_ID"               VARCHAR(2),
                                                                  "CNPJ_MAN"                VARCHAR(14),
                                                                  "ESFERAA_ID"              VARCHAR(2),
                                                                  "RETENCAO_ID"             VARCHAR(2),
                                                                  "ATIVIDAD_ID"             VARCHAR(2),
                                                                  "NATUREZA_ID"             VARCHAR(2),
                                                                  "CLIENTEL_ID"             VARCHAR(2),
                                                                  "TPUNID_ID"               VARCHAR(2),
                                                                  "TURNOAT_ID"              VARCHAR(2),
                                                                  "NIVHIER_ID"              VARCHAR(2),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "COD_CEP"                 VARCHAR(8),
                                                                  "VINC_SUS"                NUMERIC,
                                                                  "TPPREST_ID"              VARCHAR(2),
                                                                  "SGRUPHAB_ID"             VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE,
                                                                  "NATJUR_ID"               VARCHAR(4));

                       /*Tabela dos tipos de estabelecimento (só o próprio de ensino na verdade)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sgruphab_ee("ID"               VARCHAR(4),
                                                                         "TIPO"             VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_ef*/

                       /*Tabela Estabelecimentos Filantrópicos (tabela principal do sub banco de dados cnes_ef)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.efbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_EF"                   VARCHAR(2),
                                                                  "ANO_EF"                  INTEGER,
                                                                  "MES_EF"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TPGESTAO_ID"             VARCHAR(2),
                                                                  "PFPJ_ID"                 VARCHAR(2),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "NIVDEP_ID"               VARCHAR(2),
                                                                  "CNPJ_MAN"                VARCHAR(14),
                                                                  "ESFERAA_ID"              VARCHAR(2),
                                                                  "RETENCAO_ID"             VARCHAR(2),
                                                                  "ATIVIDAD_ID"             VARCHAR(2),
                                                                  "NATUREZA_ID"             VARCHAR(2),
                                                                  "CLIENTEL_ID"             VARCHAR(2),
                                                                  "TPUNID_ID"               VARCHAR(2),
                                                                  "TURNOAT_ID"              VARCHAR(2),
                                                                  "NIVHIER_ID"              VARCHAR(2),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "COD_CEP"                 VARCHAR(8),
                                                                  "VINC_SUS"                NUMERIC,
                                                                  "TPPREST_ID"              VARCHAR(2),
                                                                  "SGRUPHAB_ID"             VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE,
                                                                  "NATJUR_ID"               VARCHAR(4));

                       /*Tabela dos tipos de estabelecimento (só o próprio filantrópico na verdade)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.sgruphab_ef("ID"               VARCHAR(4),
                                                                         "TIPO"             VARCHAR(66));

                       /*Tabelas únicas do sub bancos de dados cnes_in*/

                       /*Tabela Incentivos (tabela principal do sub banco de dados cnes_in)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.inbr("CNES_ID"                 VARCHAR(7),
                                                                  "UF_IN"                   VARCHAR(2),
                                                                  "ANO_IN"                  INTEGER,
                                                                  "MES_IN"                  VARCHAR(2),
                                                                  "CODUFMUN_ID"             VARCHAR(6),
                                                                  "TPGESTAO_ID"             VARCHAR(2),
                                                                  "PFPJ_ID"                 VARCHAR(2),
                                                                  "CPF_CNPJ"                VARCHAR(14),
                                                                  "NIVDEP_ID"               VARCHAR(2),
                                                                  "CNPJ_MAN"                VARCHAR(14),
                                                                  "ESFERAA_ID"              VARCHAR(2),
                                                                  "RETENCAO_ID"             VARCHAR(2),
                                                                  "ATIVIDAD_ID"             VARCHAR(2),
                                                                  "NATUREZA_ID"             VARCHAR(2),
                                                                  "CLIENTEL_ID"             VARCHAR(2),
                                                                  "TPUNID_ID"               VARCHAR(2),
                                                                  "TURNOAT_ID"              VARCHAR(2),
                                                                  "NIVHIER_ID"              VARCHAR(2),
                                                                  "TERCEIRO"                NUMERIC,
                                                                  "COD_CEP"                 VARCHAR(8),
                                                                  "VINC_SUS"                NUMERIC,
                                                                  "TPPREST_ID"              VARCHAR(2),
                                                                  "SGRUPHAB"                VARCHAR(4),
                                                                  "CMPT_INI"                DATE,
                                                                  "CMPT_FIM"                DATE,
                                                                  "DTPORTAR"                DATE,
                                                                  "PORTARIA"                VARCHAR(50),
                                                                  "MAPORTAR"                DATE,
                                                                  "NATJUR_ID"               VARCHAR(4));
                ''')
    conn.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conn.close()

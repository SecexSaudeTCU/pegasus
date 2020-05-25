###########################################################################################################################################################################
# SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
###########################################################################################################################################################################

import psycopg2


"""
Cria o schema do banco de dados do SIA_PA (Procedimentos Ambulatoriais) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""


# Função de criação das tabelas do banco de dados sia_pa
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela Procedimentos Ambulatoriais (PA) (tabela principal do "sub" banco de dados SIA_PA)*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pabr("UF_PA"                  VARCHAR(2),
                                                                  "ANO_PA"                 INTEGER,
                                                                  "MES_PA"                 VARCHAR(2),
                                                                  "PACODUNI_ID"            VARCHAR(7),
                                                                  "PAGESTAO_ID"            VARCHAR(6),
                                                                  "PACONDIC_ID"            VARCHAR(2),
                                                                  "PAUFMUN_ID"             VARCHAR(6),
                                                                  "PAREGCT_ID"             VARCHAR(4),
                                                                  "PA_INCOUT"              NUMERIC,
                                                                  "PA_INCURG"              NUMERIC,
                                                                  "PATPUPS_ID"             VARCHAR(2),
                                                                  "PATIPPRE_ID"            VARCHAR(2),
                                                                  "PA_MN_IND"              VARCHAR(2),
                                                                  "PA_CNPJCPF"             VARCHAR(14),
                                                                  "PA_CNPJMNT"             VARCHAR(14),
                                                                  "PA_CNPJ_CC"             VARCHAR(14),
                                                                  "PA_MVM"                 DATE,
                                                                  "PA_CMP"                 DATE,
                                                                  "PAPROC_ID"              VARCHAR(10),
                                                                  "PATPFIN_ID"             VARCHAR(2),
                                                                  "PANIVCPL_ID"            VARCHAR(2),
                                                                  "PADOCORIG_ID"           VARCHAR(2),
                                                                  "PA_AUTORIZ"             VARCHAR(20),
                                                                  "PA_CNSMED"              VARCHAR(15),
                                                                  "PACBOCOD_ID"            VARCHAR(6),
                                                                  "PAMOTSAI_ID"            VARCHAR(2),
                                                                  "PA_OBITO"               NUMERIC,
                                                                  "PA_ENCERR"              NUMERIC,
                                                                  "PA_PERMAN"              NUMERIC,
                                                                  "PA_ALTA"                NUMERIC,
                                                                  "PA_TRANSF"              NUMERIC,
                                                                  "PACIDPRI_ID"            VARCHAR(4),
                                                                  "PACIDSEC_ID"            VARCHAR(4),
                                                                  "PACIDCAS_ID"            VARCHAR(4),
                                                                  "PACATEND_ID"            VARCHAR(2),
                                                                  "PA_IDADE"               FLOAT,
                                                                  "IDADEMIN"               FLOAT,
                                                                  "IDADEMAX"               FLOAT,
                                                                  "PAFLIDADE_ID"           VARCHAR(2),
                                                                  "PASEXO_ID"              VARCHAR(2),
                                                                  "PARACACOR_ID"           VARCHAR(2),
                                                                  "PAMUNPCN_ID"            VARCHAR(6),
                                                                  "PA_QTDPRO"              FLOAT,
                                                                  "PA_QTDAPR"              FLOAT,
                                                                  "PA_VALPRO"              FLOAT,
                                                                  "PA_VALAPR"              FLOAT,
                                                                  "PA_UFDIF"               NUMERIC,
                                                                  "PA_MNDIF"               NUMERIC,
                                                                  "PA_DIF_VAL"             FLOAT,
                                                                  "NU_VPA_TOT"             FLOAT,
                                                                  "NU_PA_TOT"              FLOAT,
                                                                  "PAINDICA_ID"            VARCHAR(2),
                                                                  "PACODOCO_ID"            VARCHAR(2),
                                                                  "PA_FLER"                NUMERIC,
                                                                  "PAETNIA_ID"             VARCHAR(4),
                                                                  "PA_VL_CF"               FLOAT,
                                                                  "PA_VL_CL"               FLOAT,
                                                                  "PA_VL_INC"              FLOAT,
                                                                  "PASRCC_ID"              VARCHAR(6),
                                                                  "PAINE_ID"               VARCHAR(10),
                                                                  "PANATJUR_ID"            VARCHAR(4),
                                                                  "GRUPO_ID"               VARCHAR(2),
                                                                  "SUBGRUPO_ID"            VARCHAR(4),
                                                                  "FORMA_ID"               VARCHAR(6));

                       /*Tabela dos estabelecimentos de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacoduni("ID"                 VARCHAR(7),
                                                                      "DESCESTAB"          VARCHAR(66),
                                                                      "RSOC_MAN"           VARCHAR(66),
                                                                      "CPF_CNPJ"           VARCHAR(14),
                                                                      "EXCLUIDO"           NUMERIC,
                                                                      "DATAINCL"           DATE,
                                                                      "DATAEXCL"           DATE);

                       /*Tabela dos municípios de gestão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pagestao("ID"                 VARCHAR(6),
                                                                      "MUNNOME"	           VARCHAR(66),
                                                                      "MUNNOMEX"           VARCHAR(66),
                                                                      "MUNCODDV"           VARCHAR(7),
                                                                      "OBSERV"             VARCHAR(66),
                                                                      "SITUACAO"           VARCHAR(66),
                                                                      "MUNSINP"	           VARCHAR(66),
                                                                      "MUNSIAFI"           VARCHAR(66),
                                                                      "UFCOD_ID"           VARCHAR(2),
                                                                      "AMAZONIA"           VARCHAR(66),
                                                                      "FRONTEIRA"          VARCHAR(66),
                                                                      "CAPITAL"            VARCHAR(66),
                                                                      "RSAUDE_ID"           VARCHAR(5),
                                                                      "LATITUDE"           FLOAT,
                                                                      "LONGITUDE"          FLOAT,
                                                                      "ALTITUDE"           FLOAT,
                                                                      "AREA"               FLOAT,
                                                                      "ANOINST"            VARCHAR(66),
                                                                      "ANOEXT"             VARCHAR(66),
                                                                      "SUCESSOR"           VARCHAR(66));

                       /*Tabela dos tipos de gestão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacondic("ID"                 VARCHAR(2),
                                                                      "GESTAO"             VARCHAR(66));

                       /*Tabela dos municípios de localização do estabelecimento de saúde*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paufmun("ID"                  VARCHAR(6),
                                                                     "MUNNOME"	           VARCHAR(66),
                                                                     "MUNNOMEX"            VARCHAR(66),
                                                                     "MUNCODDV"            VARCHAR(7),
                                                                     "OBSERV"              VARCHAR(66),
                                                                     "SITUACAO"            VARCHAR(66),
                                                                     "MUNSINP"	           VARCHAR(66),
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

                       /*Tabela dos tipos de regra contratual*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paregct("ID"                  VARCHAR(4),
                                                                     "REGRA"               VARCHAR(66));

                       /*Tabela dos tipos de estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.patpups("ID"                  VARCHAR(2),
                                                                     "ESTABELECIMENTO"     VARCHAR(66));

                       /*Tabela dos tipos de prestador*/
                       CREATE TABLE IF NOT EXISTS {child_db}.patippre("ID"                 VARCHAR(2),
                                                                      "ESFERA"             VARCHAR(66));

                       /*Tabela dos tipos de procedimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paproc("ID"                   VARCHAR(10),
                                                                    "PROCEDIMENTO"         VARCHAR(100));

                       /*Tabela dos tipos de financiamento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.patpfin("ID"                  VARCHAR(2),
                                                                     "FINANCIAMENTO"       VARCHAR(66));

                       /*Tabela dos níveis de complexidade*/
                       CREATE TABLE IF NOT EXISTS {child_db}.panivcpl("ID"                 VARCHAR(2),
                                                                      "COMPLEXIDADE"       VARCHAR(66));

                       /*Tabela dos tipos de sub-bancos de dados do SIA*/
                       CREATE TABLE IF NOT EXISTS {child_db}.padocorig("ID"                VARCHAR(2),
                                                                       "TIPO_SIA"          VARCHAR(66));

                       /*Tabela das ocupações*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacbocod("ID"                 VARCHAR(6),
                                                                      "OCUPACAO"           VARCHAR(66));

                       /*Tabela dos motivos de saída*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pamotsai("ID"                 VARCHAR(2),
                                                                      "MOTIVO"             VARCHAR(66));

                       /*Tabela dos diagnósticos primários*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacidpri("ID"                 VARCHAR(4),
                                                                      "DIAGNOSTICO"        VARCHAR(66));

                       /*Tabela dos diagnósticos secundários*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacidsec("ID"                 VARCHAR(4),
                                                                      "DIAGNOSTICO"        VARCHAR(66));

                       /*Tabela dos diagnósticos associados*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacidcas("ID"                 VARCHAR(4),
                                                                      "DIAGNOSTICO"        VARCHAR(66));

                       /*Tabela dos tipos de caráter do atendimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacatend("ID"                 VARCHAR(2),
                                                                      "CARATER"            VARCHAR(66));

                       /*Tabela dos tipos de compatibilidade de idade*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paflidade("ID"                VARCHAR(2),
                                                                       "COMPATIBILIDADE"   VARCHAR(66));

                       /*Tabela dos tipos de sexo*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pasexo("ID"                   VARCHAR(2),
                                                                    "SEXO"                 VARCHAR(66));

                       /*Tabela dos tipos de raça*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paracacor("ID"                VARCHAR(2),
                                                                       "RACA"              VARCHAR(66));

                       /*Tabela dos municípios de residência do paciente*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pamunpcn("ID"                 VARCHAR(6),
                                                                      "MUNNOME"	           VARCHAR(66),
                                                                      "MUNNOMEX"           VARCHAR(66),
                                                                      "MUNCODDV"           VARCHAR(7),
                                                                      "OBSERV"             VARCHAR(66),
                                                                      "SITUACAO"           VARCHAR(66),
                                                                      "MUNSINP"	           VARCHAR(66),
                                                                      "MUNSIAFI"           VARCHAR(66),
                                                                      "UFCOD_ID"           VARCHAR(2),
                                                                      "AMAZONIA"           VARCHAR(66),
                                                                      "FRONTEIRA"          VARCHAR(66),
                                                                      "CAPITAL"            VARCHAR(66),
                                                                      "RSAUDE_ID"           VARCHAR(5),
                                                                      "LATITUDE"           FLOAT,
                                                                      "LONGITUDE"          FLOAT,
                                                                      "ALTITUDE"           FLOAT,
                                                                      "AREA"               FLOAT,
                                                                      "ANOINST"            VARCHAR(66),
                                                                      "ANOEXT"             VARCHAR(66),
                                                                      "SUCESSOR"           VARCHAR(66));

                       /*Tabela dos tipos de situação da produção*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paindica("ID"                 VARCHAR(2),
                                                                      "SITUACAO"           VARCHAR(66));

                       /*Tabela dos tipos de ocorrência da produção*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pacodoco("ID"                 VARCHAR(2),
                                                                      "OCORRENCIA"         VARCHAR(66));

                       /*Tabela dos tipos de etnia indígena*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paetnia("ID"                  VARCHAR(4),
                                                                     "INDIGENA"            VARCHAR(66));

                       /*Tabela das ocupações*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pasrcc("ID"                   VARCHAR(6),
                                                                     "OCUPACAO"            VARCHAR(66));

                       /*Tabela dos tipos de equipes*/
                       CREATE TABLE IF NOT EXISTS {child_db}.paine("ID"                    VARCHAR(10),
                                                                   "EQUIPE"                VARCHAR(66));

                       /*Tabela dos tipos de equipes*/
                       CREATE TABLE IF NOT EXISTS {child_db}.panatjur("ID"                 VARCHAR(4),
                                                                      "NATUREZA"           VARCHAR(66));

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
                     CREATE TABLE IF NOT EXISTS {child_db}.forma("ID"                      VARCHAR(6),
                                                                 "FORMA"                   VARCHAR(100));

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

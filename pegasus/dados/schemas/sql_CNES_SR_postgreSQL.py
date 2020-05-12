###########################################################################################################################################################################
# CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR #
###########################################################################################################################################################################

import psycopg2


"""
Cria o schema do sub banco de dados do CNES_SR (Serviços Especializados) para atender ao formato do SGBD postgreSQL.
Na verdade o schema abaixo deve atender ao formato dos SGBD Oracle e SQL Server com no máximo pequenas modificações.

"""

# Função de criação das tabelas do sub banco de dados cnes_sr
def create_tables(connection_data, child_db):
    conn = psycopg2.connect(dbname=connection_data[0],
                            host=connection_data[1],
                            port=connection_data[2],
                            user=connection_data[3],
                            password=connection_data[4])

    cursor = conn.cursor()

    cursor.execute(f'''/*Tabela Serviços Especializados (tabela principal do sub banco de dados cnes_sr)*/
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

                       /*Tabela dos serviços especializados*/
                       CREATE TABLE IF NOT EXISTS {child_db}.servesp("ID"                   VARCHAR(3),
                                                                     "DESCRICAO"            VARCHAR(100));

                       /*Tabela de classificações dos serviços especializados*/
                       CREATE TABLE IF NOT EXISTS {child_db}.classsr("ID"                   VARCHAR(6),
                                                                     "DESCRICAO"            VARCHAR(100));

                       /*Na verdade é a mesma coisa da tabela servesp*/
                       CREATE TABLE IF NOT EXISTS {child_db}.srvunico("ID"                  VARCHAR(3),
                                                                      "DESCRICAO"           VARCHAR(100));

                       /*Tabela dos tipos de gestão*/
                       CREATE TABLE IF NOT EXISTS {child_db}.tpgestao("ID"                  VARCHAR(2),
                                                                      "GESTAO"              VARCHAR(66));

                       /*Tabela se o estabelecimento é pessoa física ou pessoa jurídica*/
                       CREATE TABLE IF NOT EXISTS {child_db}.pfpj("ID"                      VARCHAR(2),
                                                                  "PESSOA"                  VARCHAR(66));

                       /*Tabela do grau de independência do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.nivdep("ID"                    VARCHAR(2),
                                                                    "TIPO"                  VARCHAR(66));

                       /*Tabela dos tipos de administração*/
                       CREATE TABLE IF NOT EXISTS {child_db}.esferaa("ID"                   VARCHAR(2),
                                                                     "ADMINISTRACAO"        VARCHAR(66));

                       /*Tabela dos tipos de atividade de ensino, se houver*/
                       CREATE TABLE IF NOT EXISTS {child_db}.atividad("ID"                  VARCHAR(2),
                                                                      "ATIVIDADE"           VARCHAR(66));

                       /*Tabela dos tipos de retenção de tributos do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.retencao("ID"                  VARCHAR(2),
                                                                      "RETENCAO"            VARCHAR(66));

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

                       /*Tabela de caracterizações do estabelecimento*/
                       CREATE TABLE IF NOT EXISTS {child_db}.caracter("ID"                  VARCHAR(2),
                                                                      "CARACTERIZACAO"      VARCHAR(66));

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

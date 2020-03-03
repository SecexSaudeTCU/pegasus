########################################################################################################################################################################
# ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT ROOT #
########################################################################################################################################################################

import os

import psycopg2
from sqlalchemy import create_engine, event, DDL
import pandas as pd
import numpy as np

from utilities.essential_postgreSQL_a_db import files_in_ftp, get_tables_e_count_postgreSQL, files_in_postgreSQL, difference_files

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


"""
Cria um schema do banco de dados (neste computador denominado) "dbsus2" no SGBD PostgreSQL com integridade referencial
(de "primary and foreign keys") e insere nele dados dos sistemas (do Datasus) CNES, SIH, SIA, SIM, SINASC, SINAN e XXX
no SGBD PostgreSQL pelo método pandas.to_sql. Os dados são inseridos por sistema ou sub-sistema (no caso de existência
de mais de uma tabela principal): CNES_ST, CNES_DC, CNES_PF, CNES_LT, CNES_EQ, CNES_SR, CNES_EP, CNES_HB, CNES_RC, CNES_GM,
CNES_EE, CNES_EF, CNES_IN, SIH_RD, SIH_SP, SIA_PA, SIM, SINASC, SINAN_DENG e XXX. Assim, para cada sistema ou sub-sistema
do Datasus é criado um schema.

A inserção de dados consiste dos arquivos principais de dados em formato "dbc" e dos arquivos secundários de dados em
formato "dbf", "cnv" e "xlsx" dos sistemas/sub-sistemas CNES_ST (STXXaamm), CNES_DC (DCXXaamm), CNES_PF (PFXXaamm),
CNES_LT (LTXXaamm), CNES_EQ (EQXXaamm), CNES_SR (SRXXaamm), CNES_EP (EPXXaamm), CNES_HB (HBXXaamm), CNES_RC (RCXXaamm),
CNES_GM (GMXXaamm), CNES_EE (EEXXaamm), CNES_EF (EFXXaamm), CNES_IN (INXXaamm), SIH_RD (RDXXaamm), SIH_SP (SPXXaamm),
SIA_PA (PAXXaamm), SIM (DOXXaaaa), SINASC (DNXXaaaa), SINAN_DENG (DENGXXaa) e XXX. Destaca-se que alguns sistemas, como
o CNES, se subdividem em várias tabelas principais de dados, que no caso são em número de 13, conforme se pode contabilizar
acima.

Os arquivos principais de dados formam a tabela principal (child table) do respectivo sistema ou subsistema e estão em pastas
específicas do endereço ftp do Datasus (ftp://ftp.datasus.gov.br/dissemin/publicos/) em formato "dbc". Cada arquivo "dbc" é
baixado em tempo de execução, descompactado para "dbf", lido como um objeto pandas DataFrame e, para evitar a repetição do
download, é salvo numa pasta criada dinamicamente e denominada "datasus_content" no computador de execução deste script no
formato "parquet" no caso de nova necessidade desse arquivo principal de dados.

Os arquivos secundários de dados formam as tabelas relacionais (parent tables) à tabela principal e estão em formato "dbf",
"cnv" ou "xlsx". Os arquivos "dbf" e "cnv", presentes em diretórios do endereço ftp do Datasus, são baixados e convertidos
em tempo de execução para objetos pandas DataFrame enquanto os arquivos "xlsx", quando necessários, foram criados a partir
de relações descritas no Dicionário de Dados do respectivo sistema do Datasus e não retratadas em arquivos "dbf" ou "cnv" ou
a partir da incompletude de arquivos "dbf" ou "cnv".

É necessário ter as seguintes bibliotecas Python instaladas: psycopg2, SQLAlchemy, ftplib, zipfile, dbfread, xlrd, pyarrow,
fast_parquet, numpy e pandas.

Para se executar esse pacote em Python 3.7 a partir do sistema operacional Windows 10 também se instalou em C:/ o programa
TabWin do Datasus, que no seu diretório raiz contém um executável que permite a conversão de arquivos em formato "dbc" para
"dbf" denominado "dbf2dbc". O programa TabWin pode ser baixado de http://datasus1.saude.gov.br/transferencia-download...
-de-arquivos/download-do-tabwin selecionando o link "Tab415.zip" presente na primeira linha da coluna "Nome" da tabela que
aparece nessa página. As instruções de instalação presentes nessa página ensinam:

"Os arquivos compactados abaixo contêm os componentes básicos que permitem o funcionamento do Tab para Windows.
Sugerimos que você crie uma pasta, em seu computador, chamada TabWin, e copie o arquivo abaixo para essa pasta."

Por outro lado, para executar esse pacote em Python 3.7 a partir do sistema operacional Linux também se instalou a
dependência libffi ("$ sudo apt install libffi-dev") e as bibliotecas Python codecs e cffi, prescindindo, porém, da
instalação do executável "dbf2dbc". Nesse caso, se deve executar o módulo "_build_readdbc" contido no sub-package
"insertion.data_wrangling.online" para criar um wrapper de um módulo na linguagem C que descompacta arquivos "dbc" para
"dbf".

O referido diretório "datasus_content" onde são baixados os arquivos principais de dados em formato "parquet" pode ser
alterado editando o módulo "folder" contido no sub-package "insertion.data_wrangling.online".

"""


########################################################################################################################################################################
# MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN MAIN #
########################################################################################################################################################################

if __name__ == '__main__':
    print('\n***************************************************************************************')
    print('First of all, you must read the docstring provided at the beginning of this main module.\n'.upper())
    print('Also, you just do not forget that the PostgreSQL database to be filled must first be created!'.upper())
    print('Therefore the 3 constants defined at lines 155, 156 and 159 of this main module must...')
    print('first be adapted to your reality, that is: DB_USER, DB_PASS and DB_NAME; being DB_NAME...')
    print('the name of the database created in PostgreSQL.')
    print('***************************************************************************************\n')

    decision = input("You better really type 'q' if you have not done the recommended above or type anything otherwise: ")
    if decision == 'q':
        quit()
    else:
        pass

    # Nome do banco de dados do Datasus almejado
    datasus_db = input('\nEnter the Datasus database name (CNES/SIH/SIA/SIM/SINASC/SINAN/XXX): ').lower()

    # Construção do objeto string do nome do sub banco de dados do CNES ou SIH ou SIA ou SINAN ou XXX
    # CNES
    if datasus_db == 'cnes':
        print('\nST: Estabelecimentos')
        print('DC: Dados Complementares')
        print('PF: Profissionais')
        print('LT: Leitos')
        print('EQ: Equipamentos')
        print('SR: Serviços Especializados')
        print('EP: Equipes')
        print('HB: Habilitações')
        print('GM: Gestão e Metas')
        print('EE: Estabelecimentos de Ensino')
        print('EF: Estabelecimentos Filantrópicos')
        print('IN: Incentivos')
        print('RC: Regras Contratuais')
        sub_db = input('\nEnter the intended CNES "sub" database initials (ST/DC/PF/LT/EQ/SR/EP/HB/RC/GM/EE/EF/IN): ').lower()
    # SIH
    elif datasus_db == 'sih':
        print('\nRD: Autorização de Internação Hospitalar - Reduzidas')
        print('SP: Autorização de Internação Hospitalar - Serviços Profissionais')
        sub_db = input('\nEnter the intended SIH "sub" database initials (RD/SP): ').lower()
    # SIA
    elif datasus_db == 'sia':
        print('\nPA: Procedimentos Ambulatoriais')
        sub_db = input('Enter the intended SIA "sub" database initials (PA/XX): ').lower()
    # SIM ou SINASC:
    elif (datasus_db == 'sim') or (datasus_db == 'sinasc'):
        print(f'{datasus_db} is a database composed of only one main table.\n')
    # SINAN
    elif datasus_db == 'sinan':
        print('\nDENG: Dengue e Chikungunya\n')
        sub_db = input('Enter the intended SINAN "sub" database initials (DENG/XXXX): ').lower()
    datasus_db += '_' + sub_db
    print('\nTo its goal...\n')

    # Importação da variável "Base" do respectivo módulo do "datasus_db" do package "schemas"
    module1 = __import__('schemas.alchemy_declarative_' + datasus_db.upper() + '_postgreSQL', fromlist=['Base'], level=0)
    structure = module1.Base

    # Cria o schema do banco de dados child "datasus_db" do banco de dados mãe "DB_NAME" utilizando as classes definidas...
    # no respectivo módulo do package "schemas"
    event.listen(structure.metadata, 'before_create', DDL('CREATE SCHEMA IF NOT EXISTS %s' % (datasus_db)))

    # Criação de objetos string do nome das duas funções de inserção de dados do "datasus_db" contidas no respectivo...
    # módulo do package "insertion"
    str_most = 'insert_most_' + datasus_db.upper() + '_tables_pandas'
    str_main = 'insert_main_table_e_file_info_pandas'

    # Importação das duas funções de inserção de dados do "datasus_db" usando a função nativa "__import__"
    module2 = __import__('insertion.insert_into_' + datasus_db.upper(), fromlist=[str_most, str_main], level=0)

    # Colocação do nome das duas funções de inserção de dados importadas nas variáveis "most_tables" e "main_table"
    most_tables = getattr(module2, str_most)
    main_table = getattr(module2, str_main)

    # Chama a função "files_in_ftp" contida no módulo "essential_postgreSQL_a_db" do package "utilities" tendo como parâmetro...
    # a variável "datasus_db"
    df_arquivos_ftp = files_in_ftp(datasus_db)
    os.remove('stuff_ftp_files.txt')

    # Dados de conecção 1 (portanto o DB_NAME já deve ter sido previamente criado com esses dados)
    DB_TYPE = 'postgresql'
    DB_USER = 'Eric'
    DB_PASS = 'teste'
    DB_HOST = '127.0.0.1'
    DB_PORT = '5432'
    DB_NAME = 'dbsus2'      # Nome no PostgreSQL do banco de dados "espelho" do do Datasus

    try:
        # Conecta ao SGBD PostgreSQL usando o módulo python "psycopg2"
        conn = psycopg2.connect(dbname=DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
        print(conn.get_dsn_parameters(),'\n')
        # Criação de um cursor da conecção tipo "psycopg2" referenciado à variável "cursor"
        cursor = conn.cursor()
        # Chama a função "get_tables_e_count_postgreSQL" contida no módulo "essential_postgreSQL_a_db" do package "utilities"
        dict_tabelas_e_counts_pg = get_tables_e_count_postgreSQL(cursor, datasus_db)
        conn.close()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL.', error)

    # Dados de conecção 2
    DB_DRIVER = 'psycopg2'

    # URI para o PostgreSQL
    SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)

    # Cria um "engine" para o SGBD PostgreSQL usando o driver "psycopg2"
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    # Vincula o schema de "structure" definido no módulo "alchemy_declarative_'datasus_db'_postgreSQL" do package "schemas" ao banco de dados "DBNAME"
    structure.metadata.create_all(engine)

    # Chama a função "files_in_postgreSQL" contida no módulo "essential_postgreSQL_a_db" do package "utilities"
    df_arquivos_pg = files_in_postgreSQL(dict_tabelas_e_counts_pg, datasus_db, engine)
    print(df_arquivos_pg)

    # Chama a função "difference_files" contida no módulo "essential_postgreSQL_a_db" do package "utilities"
    df_arqs_nao_carregados = difference_files(df_arquivos_ftp, df_arquivos_pg)

    # Quantidade de arquivos de dados principal que falta carregar em "datasus_db"
    qtd_arqs_datasus = df_arqs_nao_carregados.shape[0]
    print(f'\nA quantidade de arquivos principais de dados do {datasus_db} que falta carregar no {DB_NAME}/PostgreSQL é {qtd_arqs_datasus}.\n')

    # Cria o objeto string "path_xlsx" do diretório onde estão os eventuais arquivos "xlsx" relativos a "datasus_db"
    if datasus_db.startswith('cnes'):
        path_xlsx = os.getcwd() + '/files/CNES/'
    elif datasus_db.startswith('sih'):
        path_xlsx = os.getcwd() + '/files/SIH/'
    elif datasus_db.startswith('sia'):
        path_xlsx = os.getcwd() + '/files/SIA/'
    elif datasus_db == 'sim':
        path_xlsx = os.getcwd() + '/files/SIM/'
    elif datasus_db == 'sinasc':
        path_xlsx = os.getcwd() + '/files/SINASC/'
    elif datasus_db.startswith('sinan'):
        path_xlsx = os.getcwd() + '/files/SINAN/'

    # Remove a única key do objeto dict "dict_tabelas_e_counts_pg" que não se refere às parent tables do "datasus_db" utilizando...
    # o método "pop" da class "dict"
    dict_tabelas_e_counts_pg.pop('arquivos', None)
    # Inicialização de contador do número de parent tables do "datasus_db" sem dados
    count = 0
    # Faz a contagem de quantas parent tables do "datasus_db" estão sem dados, se é que estão
    for table in dict_tabelas_e_counts_pg:
        if dict_tabelas_e_counts_pg[table] == 0:
            count += 1

    # Carrega dados de tabela(s) auxiliar(es) (parent table(s)) do "datasus_db" se não constar dados dela(s) no "DBNAME" do PostgreSQL
    if ((dict_tabelas_e_counts_pg == dict()) or (count > 0)):
        print(f'\nIniciando a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando pandas...')
        # Chama a função "most_tables" para inserção das tabelas auxiliares (parent tables) no "datasus_db"
        most_tables(path_xlsx, engine, datasus_db)
        print(f'Finalizou a inserção de dados auxiliares no banco de dados {datasus_db} do {DB_NAME}/PostgreSQL usando pandas.')

        # Remoção dos arquivos "cnv" baixados numa pasta zipada do endereço ftp do Datasus
        if datasus_db in np.array(['cnes_st', 'cnes_lt', 'cnes_eq', 'cnes_sr', 'cnes_ep', 'cnes_ee', 'cnes_ef', 'cnes_in']):
            os.remove('TAB_CNES.zip')
            os.rmdir('TAB_DBF_CNV')
        elif datasus_db in np.array(['sih_rd', 'sih_sp']):
            os.remove('TAB_SIH.zip')
            os.rmdir('CNV')
            os.rmdir('DBF')
        elif datasus_db in np.array(['sia_pa']):
            os.remove('TAB_SIA.zip')
            os.remove('TAB_SIH.zip')
            os.rmdir('CNV')
            os.rmdir('TAB_DBF')
        elif datasus_db == 'sim':
            os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDO18.dbf
        elif datasus_db == 'sinasc':
            os.remove('NASC_NOV_TAB.zip')
            os.remove('OBITOS_CID10_TAB.ZIP') # e do arquivo CNESDN18.dbf
        elif datasus_db == 'sinan_deng':
            os.remove('TAB_SINAN.zip')

    # Carrega dados da tabela principal do "datasus_db" no PostgreSQL
    for i in range(qtd_arqs_datasus):
        # Chama a função "main_table" para a inserção de dados na tabela principal (child table) + respectivas informações na tabela arquivos...
        # usando o pandas.to_sql "do" SQLAlchemy
        main_table(df_arqs_nao_carregados.NOME[i], df_arqs_nao_carregados.DIRETORIO[i], df_arqs_nao_carregados.DATA_INSERCAO_FTP[i],
                   engine, datasus_db, DB_NAME)

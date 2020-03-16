from datetime import datetime

import numpy as np
import pandas as pd
from ftplib import FTP

"""

"""

def get_dbc_info(ftp_path):
    # Muda para o diretório "ftp_path" onde estão os arquivos principais de dados relativos ao "folder_name"
    ftp.cwd(ftp_path)
    # Coleta os "Nome", "Tamanho" e "Data da modificação" dos arquivos contidos no diretório "ftp_path"...
    # como um objeto string e o coloca no arquivo "stuff_ftp_files.txt"
    ftp.retrlines('LIST', open('stuff_ftp_files.txt', 'w').write)
    # Abre o arquivo "stuff_ftp_files.txt" num objeto file handler referenciado à variável "f"
    with open('stuff_ftp_files.txt') as f:
        # Lê de uma vez o conteúdo do arquivo "stuff_ftp_files.txt" guardando-o num objeto do tipo string...
        # e referenciando-o à variável "string_ftp_files"
        string_ftp_files = f.read()
    # Divide o conteúdo da variável "string_ftp_files" (tornada UPPER CASE) onde haja a substring ".DBC",...
    # colocando cada fatia como um objeto string de um objeto list e desconsiderando o último elemento string...
    # da list por ser uma string vazia já que o objeto string de "string_ftp_files" termina com ".DBC"
    list_ftp_files = string_ftp_files.upper().split('.DBC')[:-1]
    # Concatena ao final de cada elemento string da variável list "list_ftp_files" a string ".dbc"...
    for i in range(len(list_ftp_files)):
        list_ftp_files[i] = list_ftp_files[i] + '.dbc'
    # Cria um objeto list vazio destinado ao nome dos arquivos de extensão "dbc"
    list_file_names = []
    # Cria um objeto list vazio destinado à data de inserção do arquivo "dbc" no diretório ftp
    list_file_dates = []
    # Iterate por cada objeto string do objeto list "list_ftp_files", divide-o nos espaços vazios colocando...
    # cada fatia como um objeto string de um objeto list e pega apenas o primeiro e o último elemento desse...
    # objeto list colocando como um elemento da list "list_file_names"
    for elem in list_ftp_files:
        list_file_dates.append(datetime.strptime(elem.split()[0], '%m-%d-%y').date()) # Já converte o objeto string para date
        list_file_names.append(elem.split()[-1])

    # Cria um objeto pandas DataFrame para armazenar o nome dos arquivos de dados "dbc", o diretório onde o...
    # arquivo se encontra e a data de inserção ou reinserção do arquivo no diretório ftp
    df_files = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP'])
    # Aloca os dados no objeto pandas DataFrame
    for i in np.arange(len(list_file_names)):
        df_files.loc[i] = [list_file_names[i], ftp_path, list_file_dates[i]]

    return df_files


def files_in_ftp(child_db):
    # Host domain
    host = 'ftp.datasus.gov.br'
    # Criação de objeto FTP como uma variável global
    global ftp
    ftp = FTP(host)
    # Realização de conexão
    ftp.login()

    # CNES
    if child_db == 'cnes':
        # Diretório do host onde estão os dados do banco de dados "child_db" (CNES) do Datasus
        ftp_path = '/dissemin/publicos/CNES/200508_/Dados/'
        # Muda para o diretório "ftp_path" que contém 13 pastas com os arquivos principais de dados
        ftp.cwd(ftp_path)
        # Obtém o nome dessas 13 pastas do diretório "ftp_path" como um objeto list
        lista_folder_names = ftp.nlst()
        # Cria objeto list vazio
        frames = []
        # Itera sobre cada diretório dos 13 diretórios de dados principais do CNES
        for folder_name in lista_folder_names:
            # Cria o objeto string final do path do diretório onde estão os arquivos principais de dados...
            # relativos ao "folder_name" e o armazena na variável "datasus_path"
            datasus_path = ftp_path + folder_name + '/'
            # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
            # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
            dfi = get_dbc_info(datasus_path)
            # Adiciona um objeto pandas DataFrame como elem do objeto list "frames"
            frames.append(dfi)
        # Concatena todos os objetos pandas DataFrame que são elem de "frames" como o objeto pandas DataFrame "df_ftp"
        df_ftp = pd.concat(frames, ignore_index=True)
        # Considera apenas as linhas de "df_ftp" cuja coluna "NOME" começa pelas string "DC" ou "EE" ... ou "ST"
        df_ftp = df_ftp[df_ftp['NOME'].str.match(pat='(^DC)|(^EE)|(^EF)|(^EP)|(^EQ)|(^GM)|(^HB)|(^IN)|(^LT)|(^PF)|(^RC)|(^SR)|(^ST)')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira aos anos de 2007
        df_ftp = df_ftp[~df_ftp['NOME'].str.match(pat='(^EE.{2}07)|(^EF.{2}07)|(^EP.{2}07)|(^HB.{2}07)|(^IN.{2}07)|(^RC.{2}07)')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" inicie por "GM" e se refira aos anos de 2007 a 2014
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^GM.{2}0[7-9].{2}', regex=True)]
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^GM.{2}1[0-4].{2}', regex=True)]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira aos anos de 2005
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}05', regex=True)]

        # Desconsidera arquivos a partir de 2020: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SIH
    elif child_db == 'sih':
        # Diretório do host onde estão os dados do banco de dados "child_db" (SIH) do Datasus
        datasus_path = '/dissemin/publicos/SIHSUS/200801_/Dados/'
        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)
        # Considera apenas as linhas de "df_ftp" cuja coluna "NOME" começa pelas string "RD" ou "SP"
        df_ftp = df_ftp[df_ftp['NOME'].str.match(pat='(^RD)|(^SP)')]
        # Desconsidera a linha de "df_ftp" cuja coluna "NOME" contenha "RDAC0909" por estar sem dado esse arquivo
        df_ftp = df_ftp[~df_ftp['NOME'].str.startswith('RDAC0909')]

        # Desconsidera arquivos a partir de 2020: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SIM
    elif child_db == 'sim':
        # Diretório do host onde estão os dados do banco de dados "child_db" (SIM) do Datasus
        datasus_path = '/dissemin/publicos/SIM/CID10/DORES/'
        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)
        # Considera apenas as linhas de "df_ftp" cuja coluna "NOME" começa pela string "DO"
        df_ftp = df_ftp[df_ftp['NOME'].str.startswith('DO')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira ao ano de 1996
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('1996')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" a string "BR"
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('BR')]

        # Desconsidera arquivos a partir de 2018: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^DO.{2}201[8-9]', regex=True)]

    # SINASC
    elif child_db == 'sinasc':
        # Diretório do host onde estão os dados do banco de dados "child_db" (SINASC) do Datasus
        datasus_path = '/dissemin/publicos/SINASC/NOV/DNRES/'
        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)
        # Considera apenas as linhas de "df_ftp" cuja coluna "NOME" começa pela string "DN"
        df_ftp = df_ftp[df_ftp['NOME'].str.startswith('DN')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira ao ano de 1996
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('1996')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" a string "BR"
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('BR')]

        # Desconsidera arquivos a partir de 2018: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^DN.{2}201[8-9]', regex=True)]

    # Reset o index devido à eventual eliminação de linhas
    df_ftp.reset_index(drop=True, inplace=True)

    # Encerra conexão
    ftp.quit()

    return df_ftp


def get_tables_e_count_postgreSQL(pointer, child_db):

    if child_db == 'cnes':
        main_tables = ['dcbr', 'eebr', 'efbr', 'epbr', 'eqbr', 'gmbr', 'hbbr',
                       'inbr', 'ltbr', 'pfbr', 'rcbr', 'srbr', 'stbr']

    elif child_db == 'sih':
        main_tables = ['rdbr', 'spbr']
    elif child_db == 'sim':
        main_tables = ['dobr']
    elif child_db == 'sinasc':
        main_tables = ['dnbr']
    elif child_db == 'sinan':
        main_tables = ['dengbr']

    # Solicita a estrutura de tabelas de um banco de dados no PostgreSQL
    pointer.execute('''SELECT table_schema, table_name, ordinal_position as position, column_name, data_type,
                       case when character_maximum_length is not null
                       then character_maximum_length
                       else numeric_precision end as max_length,
                       is_nullable,
                       column_default as default_value
                       from information_schema.columns
                       where table_schema not in ('information_schema', 'pg_catalog')
                       order by table_schema,
                       table_name,
                       ordinal_position;''')
    # O banco de dados existe no PostgreSQL mas está vazio
    if pointer.rowcount == 0:
        print('\nThere is such a database in PostgreSQL but it has no tables.')
        return dict()
    # O banco de dados existe no PostgreSQL e não está vazio, mas pode estar vazio de dados
    else:
        print('\nThe database structure in PostgreSQL is shown below:\n')
        dict_tables_pg = {}
        # Iteração sobre cada linha do comando executado pelo "pointer"
        for row in pointer:
            # Impressão na tela de cada "row" do comando executado pelo "pointer"
            print('%r' % (row,))
            if row[0] == child_db:
                if row[1] not in dict_tables_pg:
                    # Preenche o objeto dict tendo como key o nome da tabela "row[1]" e como value o nome da primeira coluna ("row[3]")
                    dict_tables_pg.update({row[1]: row[3]})

        # Remove apenas a key da tabela principal do "child_db"
        for main_table in main_tables:
            dict_tables_pg.pop(main_table, None)
        # Cria um objeto dict para alojar como key o nome de cada tabela contida em "dict_tables_pg" e como value a quantidade de registros da tabela
        dict_tables_size_pg = {}
        # Iteração sobre cada nome de tabela contido em "dict_tables_pg"
        for table in dict_tables_pg:
            # Obtém a contagem de registros da tabela "table" do "child_db"
            pointer.execute('''SELECT COUNT('%s') FROM %s.%s''' % (dict_tables_pg[table], child_db, table))
            for row in pointer:
                # Coleta o número de registros contidos em "table"
                size = row[0]
                # Preenche um objeto dict tendo como key o nome da tabela "table" e como value o número de registros "size"
                dict_tables_size_pg.update({table: size})
        return dict_tables_size_pg


def files_in_postgreSQL(structure_pg, child_db, e):
    # O schema do "name_db_pg" não foi inserido no PostgreSQL
    if structure_pg == dict():
        print('\nThe schema of the PostgreSQL database has no tables at all!')
        # Cria um objeto pandas DataFrame vazio com as colunas especificadas
        df_files_pg = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'])
        # Coloca a coluna NOME como index do objeto pandas DataFrame
        df_files_pg.set_index('NOME')
        return df_files_pg
    # Tabela "arquivos" não constante ainda do "name_db_pg" do PostgreSQL
    elif 'arquivos' not in structure_pg:
        print('\nThe schema of the PostgreSQL database does not contain the table arquivos.')
        # Cria um objeto pandas DataFrame vazio com as colunas especificadas
        df_files_pg = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'])
        # Coloca a coluna NOME como index do objeto pandas DataFrame
        df_files_pg.set_index('NOME')
        return df_files_pg
    # Tabela "arquivos" já constante do "name_db_pg" do PostgreSQL
    elif 'arquivos' in structure_pg:
        print('\nThe schema of the PostgreSQL database contains the table arquivos.')
        # Coleta os nomes dos arquivos de dados da "name_db_pg" constantes do PostgreSQL e suas informações...
        # como um objeto pandas DataFrame
        df_files_pg = pd.read_sql('''SELECT * FROM %s.arquivos''' % (child_db), con=e, index_col='NOME')
        if df_files_pg.shape[0] == 0:
            print('\nThe PostgreSQL database doest not contain main data file loaded.')
        else:
            print('\nThe PostgreSQL database contains main data file loaded.')
        return df_files_pg


def difference_files(df_files_ftp, df_files_pg):
    # Objeto list de nomes de arquivos de dados "dbc" do banco de dados "datasus_db" do Datasus contidos no...
    # diretório do seu endereço ftp referenciado à variável "datasus_path" e não presentes no banco...
    # de dados "DB_NAME" do PostgreSQL
    list_files_exclusivos_ftp = list(set(df_files_ftp.NOME.tolist()) - set(df_files_pg.index.tolist()))
    # Criação de objeto pandas DataFrame para conter a listagem de nomes de arquivos de dados "dbc" que faltam ser...
    # carregados contendo ainda o diretório e a data de inserção de cada arquivo de dados "dbc"
    df_difference = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP'])
    # Iteração sobre cada nome de arquivo "dbc" que falta ser carregado
    for file_name in list_files_exclusivos_ftp:
        # Coleta a linha do objeto pandas DataFrame "df_files_ftp" que contém na coluna Arquivo o valor "file_name"
        row = df_files_ftp.loc[df_files_ftp['NOME'] == file_name]
        # Aloca a linha "row" no objeto pandas DataFrame "df_difference"
        df_difference = df_difference.append(row, ignore_index=True)
    # Ordena as linhas de "df_difference" por ordem crescente dos valores das colunas NOME e DIRETORIO
    df_difference.sort_values(['NOME', 'DIRETORIO'], inplace=True)
    # Reset o index devido ao sorting prévio
    df_difference.reset_index(drop=True, inplace=True)
    return df_difference


# Função de eliminação de linhas de um objeto pandas DataFrame cujos valores das colunas especificadas coincidem...
# com valores das mesmas colunas de uma tabela do banco de dados
def clean_df_db_duplicates(df, tablename, e, dup_cols=[]):

    """
    Remove linhas de um objeto pandas DataFrame que já existem num banco de dados.
    Requerido:
    -df: objeto pandas DataFrame a remover linhas duplicadas nele mesmo e duplicadas na respectiva
         tabela do banco de dados;
    -engine: objeto engine do SQLAlchemy;
    - tablename: nome da tabela no banco de dados a checar linhas duplicadas;
    - dup_cols: objeto list ou tuple dos nomes das colunas a checar por valores de linhas duplicadas;
    Retorna:
    -Objeto pandas Dataframe com valores nas colunas "dup_cols" que não constam da respectiva
     tabela do banco de dados

    """

    # Obtém os valores da(s) coluna(s) da tabela do banco de dados que se deseja verificar a duplicação com o objeto pandas DataFrame
    args = '''SELECT %s FROM %s''' % (', '.join(['"{}"'.format(col) for col in dup_cols]), tablename)
    df = pd.merge(df, pd.read_sql(args, e), how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df

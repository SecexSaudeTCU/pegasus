from datetime import datetime

import numpy as np
import pandas as pd
from ftplib import FTP

"""
Módulo com funções para obtenção de:

1) lista de nomes (e outros metadados) dos arquivos principais de dados de base de dados constante de um
diretório do servidor FTP do Datasus [get_dbc_info];

2) seleciona determinadas linhas da lista de nomes (e outros metadados) dos arquivos principais de dados de
uma base ou uma sub-base de dados do Datasus [files_in_ftp_base/files_in_ftp_subbase];

3) dicionário dos nomes das tabelas auxiliares e o respectivo número de registros constantes de um banco ou
sub-banco de dados PostgreSQL [get_tables_counts_db/get_tables_counts_subdb];

4) dataframe com os nomes (e outros metadados) dos arquivos principais de dados de base de dados do Datasus
já carregados no respectivo banco de dados PostgreSQL [files_loaded];

5) dataframe com os nomes (e outros metadados) dos arquivos principais de dados de base de dados do Datasus
 que ainda faltam inserir no respectivo banco de dados PostgreSQL [files_to_load];
"""


def get_dbc_info(ftp_path):
    """
    Criação de objeto pandas DataFrame contendo nomes (e outros metadados) dos arquivos principais de dados
    de base de dados do Datasus constante do diretório FTP "ftp_path".

    Parâmetros
    ----------
    ftp_path: objeto str
        String do diretório FTP

    Retorno
    -------
    df_files: objeto pandas Dataframe
        Dataframe tendo como colunas o nome, o diretório e a data de inserção de cada arquivo principal de
        um diretório FTP do Datasus
    """

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
        list_file_dates.append(datetime.strptime(elem.split()[0], '%m-%d-%y').date()) # Já converte o objeto string
                                                                                      # para date
        list_file_names.append(elem.split()[-1])

    # Cria um objeto pandas DataFrame para armazenar o nome dos arquivos de dados "dbc", o diretório onde o...
    # arquivo se encontra e a data de inserção ou reinserção do arquivo no diretório ftp
    df_files = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP'])
    # Aloca os dados no objeto pandas DataFrame
    for i in np.arange(len(list_file_names)):
        df_files.loc[i] = [list_file_names[i], ftp_path, list_file_dates[i]]

    return df_files


def files_in_ftp_base(name_base):
    """
    Seleciona determinadas linhas do objeto pandas DataFrame retornado pela função "get_dbc_info" e relativo
    a uma base de dados do Datasus denominada "name_base" em lowercase.

    Parâmetros
    ----------
    name_base: objeto str
        String do nome (lowercase) de uma base de dados do Datasus

    Retorno
    -------
    df_ftp: objeto pandas Dataframe
        Dataframe tendo como colunas o nome, o diretório e a data de inserção dos arquivos principais de dados
        selecionados de uma base de dados do Datasus
    """

    # Host domain
    host = 'ftp.datasus.gov.br'
    # Criação de objeto FTP como uma variável global
    global ftp
    ftp = FTP(host)
    # Realização de conexão
    ftp.login()

    # CNES
    if name_base == 'cnes':
        # Diretório do host onde estão os dados da base de dados "name_base" (CNES) do Datasus
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
        df_ftp = df_ftp[df_ftp['NOME'].str.match(pat='(^DC)|(^EE)|(^EF)|(^EP)|(^EQ)|(^GM)|(^HB)|\
                                                      (^IN)|(^LT)|(^PF)|(^RC)|(^SR)|(^ST)')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira aos anos de 2007
        df_ftp = df_ftp[~df_ftp['NOME'].str.match(pat='(^EE.{2}07)|(^EF.{2}07)|(^EP.{2}07)|\
                                                       (^HB.{2}07)|(^IN.{2}07)|(^RC.{2}07)')]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" inicie por "GM" e se refira aos anos de 2007 a 2014
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^GM.{2}0[7-9].{2}', regex=True)]
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^GM.{2}1[0-4].{2}', regex=True)]
        # Desconsidera as linhas de "df_ftp" cuja coluna "NOME" se refira aos anos de 2005
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}05', regex=True)]
        # Desconsidera arquivos a partir de 2020: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SIH
    elif name_base == 'sih':
        # Diretório do host onde estão os dados da base de dados "name_base" (SIH) do Datasus
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
    elif name_base == 'sim':
        # Diretório do host onde estão os dados da base de dados "name_base" (SIM) do Datasus
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
    elif name_base == 'sinasc':
        # Diretório do host onde estão os dados da base de dados "name_base" (SINASC) do Datasus
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


def files_in_ftp_subbase(name_subbase):
    """
    Seleciona determinadas linhas do objeto pandas DataFrame retornado pela função "get_dbc_info" e relativo
    a uma sub-base de dados do Datasus denominada "name_subbase" em lowercase.

    Parâmetros
    ----------
    name_subbase: objeto str
        String do nome (lowercase) de uma sub-base de dados do Datasus

    Retorno
    -------
    df_ftp: objeto pandas Dataframe
        Dataframe tendo como colunas o nome, o diretório e a data de inserção dos arquivos principais de dados
        selecionados de uma sub-base de dados do Datasus
    """

    # Host domain
    host = 'ftp.datasus.gov.br'
    # Criação de objeto FTP como uma variável global
    global ftp
    ftp = FTP(host)
    # Realização de conexão
    ftp.login()

    # CNES
    if name_subbase.startswith('cnes'):
        # Diretório do host onde estão os dados da sub-base de dados "name_subbase" (integrante do CNES) do Datasus
        folder = name_subbase[-2:].upper() + '/'
        datasus_path = '/dissemin/publicos/CNES/200508_/Dados/' + folder

        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)

        if name_subbase[-2:] in np.array(['ep', 'hb', 'rc', 'ee', 'ef', 'in']):
            # Desconsideração das linhas de "df_ftp" representativa de quaisquer tipos de arquivos principais de...
            # dados do cnes_ep, cnes_hb, cnes_rc, cnes_ee, cnes_ef, cnes_in do ano de 2007
            df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}07.{2}', regex=True)]
        elif name_subbase[-2:] in np.array(['gm']):
            # Desconsideração das linhas de "df_ftp" representativa de quaisquer tipos de arquivos principais de...
            # dados do cnes_gm dos anos de 2007 a 2014
            df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}0[7-9].{2}', regex=True)]
            df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}1[0-4].{2}', regex=True)]
        else:
            # Desconsideração das linhas de "ddf_ftp" representativa de quaisquer tipos de arquivos principais de...
            # dados do CNES do ano de 2005, exceto do cnes_ep, cnes_hb, cnes_rc, cnes_gm, cnes_ee, cnes_ef, cnes_in
            df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}05.{2}', regex=True)]

        # Desconsidera arquivos a partir de 2020: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SIH
    elif name_subbase.startswith('sih'):
        # Diretório do host onde estão os dados da sub-base de dados "name_subbase" (integrante do SIH) do Datasus
        datasus_path = '/dissemin/publicos/SIHSUS/200801_/Dados/'

        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)

        if name_subbase == 'sih_rd':
            # Consideração apenas das linhas do objeto pandas DataFrame "df_ftp" cuja coluna NOME inicie pela string...
            # "RD" relativa ao banco de dados das AIH Reduzidas
            df_ftp = df_ftp[df_ftp['NOME'].str.startswith('RD')]
            # Sem dados esse arquivo
            df_ftp = df_ftp[~df_ftp['NOME'].str.startswith('RDAC0909')]
        elif name_subbase == 'sih_sp':
            # Consideração apenas das linhas do objeto pandas DataFrame "df_ftp" cuja coluna NOME inicie pela string...
            # "SP" relativa ao banco de dados das AIH Reduzidas
            df_ftp = df_ftp[df_ftp['NOME'].str.startswith('SP')]

        # Desconsidera arquivos a partir de 2020: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SIA
    elif name_subbase.startswith('sia'):
        # Diretório do host onde estão os dados da sub-base de dados "name_subbase" (integrante do SIA) do Datasus
        datasus_path = '/dissemin/publicos/SIASUS/200801_/Dados/'

        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)

        if name_subbase == 'sia_pa':
            # Consideração apenas das linhas do objeto pandas DataFrame "df_ftp" cuja coluna NOME inicie pela string...
            # "PA" relativa ao banco de dados dos Procedimentos Ambulatoriais
            df_ftp = df_ftp[df_ftp['NOME'].str.startswith('PA')]
            # Adequa no nome de alguns arquivos "dbc" que terminam excepcionalmente em "A" ou "B" transformando...
            # respectivamente para "a" ou "b"
            df_ftp['NOME'].replace(regex='PASP1112A', value='PASP1112a', inplace=True)
            df_ftp['NOME'].replace(regex='PASP1112B', value='PASP1112b', inplace=True)
            df_ftp['NOME'].replace({r'^(PASP1[3-9]0[1-9])A(\.dbc)' : r'\1a\2'}, regex=True, inplace=True)
            df_ftp['NOME'].replace({r'^(PASP1[3-9]1[0-2])A(\.dbc)' : r'\1a\2'}, regex=True, inplace=True)
            df_ftp['NOME'].replace({r'^(PASP1[3-9]0[1-9])B(\.dbc)' : r'\1b\2'}, regex=True, inplace=True)
            df_ftp['NOME'].replace({r'^(PASP1[3-9]1[0-2])B(\.dbc)' : r'\1b\2'}, regex=True, inplace=True)

        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^.{4}20', regex=True)]

    # SINAN
    elif name_subbase.startswith('sinan'):
        # Diretório do host onde estão os dados da sub-base de dados "name_subbase" (integrante do SINAN) do Datasus
        datasus_path = '/dissemin/publicos/SINAN/DADOS/FINAIS/'

        # Chama a função "get_dbc_info" para colocar o nome, o diretório e a data da inserção do arquivo no...
        # endereço ftp como colunas de um objeto pandas DataFrame e os preenche com os dados de "stuff_ftp_files.txt"
        df_ftp = get_dbc_info(datasus_path)

        # Consideração apenas das linhas do objeto pandas DataFrame "df_ftp" cuja coluna NOME inicie pela string...
        # "DENG" relativa ao banco de dados dos agravos dengue e chikungunya
        df_ftp = df_ftp[df_ftp['NOME'].str.startswith('DENG')]

        # Desconsidera arquivos a partir de 2018: data wrangling ainda não realizado
        df_ftp = df_ftp[~df_ftp['NOME'].str.contains('^DENG.{2}1[8-9]', regex=True)]

    ftp.quit()

    return df_ftp


def get_tables_counts_db(pointer, name_db):
    """
    Cria dicionário dos nomes das tabelas auxiliares com o respectivo número de registros constantes de um
    banco de dados PostgreSQL.

    Parâmetros
    ----------
    pointer: objeto cursor
        Cursor de uma conexão psycopg2 com um banco de dados PostgreSQL
    name_db: objeto str
        String do nome de um banco de dados PostgreSQL

    Retorno
    -------
    dict_tables_size_pg: objeto dict
        Dicionário Python tendo como keys os nomes de tabelas auxiliares de um banco de dados PostgreSQL
        e tendo como values as respectivas quantidades de registros
    """

    if name_db == 'cnes':
        main_tables = ['dcbr', 'eebr', 'efbr', 'epbr', 'eqbr', 'gmbr', 'hbbr',
                       'inbr', 'ltbr', 'pfbr', 'rcbr', 'srbr', 'stbr']
    elif name_db == 'sih':
        main_tables = ['rdbr', 'spbr']
    elif name_db == 'sim':
        main_tables = ['dobr']
    elif name_db == 'sinasc':
        main_tables = ['dnbr']
    elif name_db == 'sinan':
        main_tables = ['dengbr']

    # Solicita a estrutura de tabelas de um banco de dados no PostgreSQL
    pointer.execute('''SELECT table_schema, table_name, ordinal_position AS position, column_name, data_type,
                              CASE WHEN character_maximum_length IS NOT NULL
                              THEN character_maximum_length
                              ELSE numeric_precision END AS max_length,
                              is_nullable,
                              column_default AS default_value
                       FROM information_schema.columns
                       WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                       ORDER BY table_schema, table_name, ordinal_position;''')
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
            if row[0] == name_db:
                if row[1] not in dict_tables_pg:
                    # Preenche o objeto dict tendo como key o nome da tabela "row[1]" e...
                    # tendo como value o nome da primeira coluna ("row[3]")
                    dict_tables_pg.update({row[1]: row[3]})

        # Remove apenas as keys das tabelas principais do "name_db"
        for main_table in main_tables:
            dict_tables_pg.pop(main_table, None)
        # Cria um objeto dict para alojar como key o nome de cada tabela contida em "dict_tables_pg" e...
        # como value a quantidade de registros da tabela
        dict_tables_size_pg = {}
        # Iteração sobre cada nome de tabela contido em "dict_tables_pg"
        for table in dict_tables_pg:
            # Obtém a contagem de registros da tabela "table" do "name_db"
            pointer.execute(f'''SELECT COUNT('{dict_tables_pg[table]}') FROM {name_db}.{table}''')
            for row in pointer:
                # Coleta o número de registros contidos em "table"
                size = row[0]
                # Preenche um objeto dict tendo como key o nome da tabela "table" e...
                # como value o número de registros "size"
                dict_tables_size_pg.update({table: size})
        return dict_tables_size_pg


def get_tables_counts_subdb(pointer, name_subdb):
    """
    Cria dicionário dos nomes das tabelas auxiliares com o respectivo número de registros constantes de um
    sub-banco de dados PostgreSQL.

    Parâmetros
    ----------
    pointer: objeto cursor
        Cursor de uma conexão psycopg2 com um sub-banco de dados PostgreSQL
    name_subdb: objeto str
        String do nome de um sub-banco de dados PostgreSQL

    Retorno
    -------
    dict_tables_size_pg: objeto dict
        Dicionário Python tendo como keys os nomes de tabelas auxiliares de um sub-banco de dados PostgreSQL
        e tendo como values as respectivas quantidades de registros
    """

    if name_subdb.startswith('cnes') or name_subdb.startswith('sih') or name_subdb.startswith('sia'):
        main_table = name_subdb[-2:] + 'br'
    elif name_subdb == 'sim':
        main_table = 'dobr'
    elif name_subdb == 'sinasc':
        main_table = 'dnbr'
    elif name_subdb == 'sinan_deng':
        main_table = 'dengbr'

    # Solicita a estrutura de tabelas de um banco de dados no PostgreSQL
    pointer.execute('''SELECT table_schema, table_name, ordinal_position AS position, column_name, data_type,
                              CASE WHEN character_maximum_length IS NOT NULL
                              THEN character_maximum_length
                              ELSE numeric_precision END AS max_length,
                              is_nullable,
                              column_default AS default_value
                       FROM information_schema.columns
                       WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                       ORDER BY table_schema, table_name, ordinal_position;''')
    # O banco de dados existe no PostgreSQL mas está vazio
    if pointer.rowcount == 0:
        print('\nThere is such a database in PostgreSQL but it has no tables.')
        return dict()
    # O banco de dados existe no PostgreSQL e não está vazio
    else:
        print('\nThe database structure in PostgreSQL is shown below:\n')
        dict_tables_pg = {}
        # Iteração sobre cada linha do comando executado pelo "pointer"
        for row in pointer:
            # Impressão na tela de cada "row" do comando executado pelo "pointer"
            print('%r' % (row,))
            if row[0] == name_subdb:
                if row[1] not in dict_tables_pg:
                    # Preenche o objeto dict tendo como key o nome da tabela "row[1]" e como value o nome da...
                    # primeira coluna ("row[3]")
                    dict_tables_pg.update({row[1]: row[3]})

        # Remove apenas a key da tabela principal do "name_subdb"
        dict_tables_pg.pop(main_table, None)
        # Cria um objeto dict para alojar como key o nome de cada tabela contida em "dict_tables_pg" e como...
        # value a quantidade de registros da tabela
        dict_tables_size_pg = {}
        # Iteração sobre cada nome de tabela contido em "dict_tables_pg"
        for table in dict_tables_pg:
            # Obtém a contagem de registros da tabela "table" do "name_subdb"
            pointer.execute(f'''SELECT COUNT('{dict_tables_pg[table]}') FROM {name_subdb}.{table}''')
            for row in pointer:
                # Coleta o número de registros contidos em "table"
                size = row[0]
                # Preenche um objeto dict tendo como key o nome da tabela "table" e como value o número de...
                # registros "size"
                dict_tables_size_pg.update({table: size})
        return dict_tables_size_pg


def files_loaded(structure_pg, name_db, e):
    """
    Obtém lista de nomes (e outros metadados) dos arquivos principais de dados eventualmente carregados num
    banco ou sub-banco de dados PostgreSQL.

    Parâmetros
    ----------
    structure_pg: objeto dict
        Dicionário Python tendo como keys os nomes de tabelas auxiliares de um banco ou sub-banco de dados
        PostgreSQL e tendo como values as respectivas quantidades de registros
    name_db: objeto str
        String do nome de um banco ou sub-banco de dados PostgreSQL
    e: objeto engine
        Engine de comunicação com um banco ou sub-banco de dados PostgreSQL através do driver psycopg2

    Retorno
    -------
    df_files_pg: objeto pandas DataFrame
        Dataframe contendo nomes (e outros metadados) dos arquivos principais de dados eventualmente carregados
        num banco ou sub-banco de dados PostgreSQL
    """

    # O schema do "name_db" não foi inserido no PostgreSQL
    if structure_pg == dict():
        print('\nThe schema of the PostgreSQL database has no tables at all!')
        # Cria um objeto pandas DataFrame vazio com as colunas especificadas
        df_files_pg = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'])
        # Coloca a coluna NOME como index do objeto pandas DataFrame
        df_files_pg.set_index('NOME')
        return df_files_pg
    # Tabela "arquivos" não constante ainda do "name_db" do PostgreSQL
    elif 'arquivos' not in structure_pg:
        print('\nThe schema of the PostgreSQL database does not contain the table arquivos.')
        # Cria um objeto pandas DataFrame vazio com as colunas especificadas
        df_files_pg = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'])
        # Coloca a coluna NOME como index do objeto pandas DataFrame
        df_files_pg.set_index('NOME')
        return df_files_pg
    # Tabela "arquivos" já constante do "name_db" do PostgreSQL
    elif 'arquivos' in structure_pg:
        print('\nThe schema of the PostgreSQL database contains the table arquivos.')
        # Coleta os nomes dos arquivos de dados da "name_db" constantes do PostgreSQL e suas informações...
        # como um objeto pandas DataFrame
        df_files_pg = pd.read_sql(f'''SELECT * FROM {name_db}.arquivos''', con=e, index_col='NOME')
        if df_files_pg.shape[0] == 0:
            print('\nThe PostgreSQL database doest not contain main data file loaded.')
        else:
            print('\nThe PostgreSQL database contains main data file loaded.')
        return df_files_pg


def files_to_load(df_files_ftp, df_files_pg):
    """
    Obtém lista de nomes (e outros metadados) dos arquivos principais de dados que faltam carregar num
    banco ou sub-banco de dados PostgreSQL.

    Parâmetros
    ----------
    df_files_ftp: objeto pandas DataFrame
        Dataframe contendo nomes (e outros metadados) dos arquivos principais de dados selecionados de uma base
        de dados do Datasus
    df_files_pg: objeto pandas DataFrame
        Dataframe contendo nomes (e outros metadados) dos arquivos principais de dados eventualmente carregados
        num banco ou sub-banco de dados PostgreSQL

    Retorno
    -------
    df_difference: objeto pandas DataFrame
        Dataframe contendo nomes (e outros metadados) dos arquivos principais de dados que faltam carregar num
        banco ou sub-banco de dados PostgreSQL
    """

    # Objeto list de nomes de arquivos de dados "dbc" do banco de dados "datasus_db" do Datasus contidos no...
    # diretório do seu endereço ftp referenciado à variável "datasus_path" e não presentes no banco...
    # de dados "DB_NAME" do PostgreSQL
    list_files_exclusivos_ftp = list(set(df_files_ftp.NOME.tolist()) - set(df_files_pg.index.tolist()))
    # Criação de objeto pandas DataFrame para conter a listagem de nomes de arquivos de dados "dbc" que faltam ser...
    # carregados contendo ainda o diretório e a data de inserção de cada arquivo de dados "dbc"
    df_difference = pd.DataFrame(columns=['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP'])
    # Iteração sobre cada nome de arquivo "dbc" que falta ser carregado
    for file_name in list_files_exclusivos_ftp:
        # Coleta a linha do objeto pandas DataFrame "df_files_ftp" que contém na coluna NOME o valor "file_name"
        row = df_files_ftp.loc[df_files_ftp['NOME'] == file_name]
        # Aloca a linha "row" no objeto pandas DataFrame "df_difference"
        df_difference = df_difference.append(row, ignore_index=True)
    # Ordena as linhas de "df_difference" por ordem crescente dos valores das colunas NOME e DIRETORIO
    df_difference.sort_values(['NOME', 'DIRETORIO'], inplace=True)
    # Reset o index devido ao sorting prévio
    df_difference.reset_index(drop=True, inplace=True)
    return df_difference

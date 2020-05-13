###############################################################################################################################################################
# SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA #
###############################################################################################################################################################

import os

import numpy as np
import pandas as pd
from ftplib import FTP
from zipfile import ZipFile
from dbfread import DBF

from .folder import CACHEPATH

if os.name == 'nt':
    from .read_windows import read_dbc, read_cnv
elif os.name == 'posix':
    from .read_unix import read_dbc, read_cnv

"""
Lê arquivos principais de dados do SIA (PAXXaamm = Procedimentos Ambulatoriais, XXX) constante do endereço
ftp do Datasus em formato "dbc" como um objeto pandas DataFrame e o salva no formato "parquet". Caso o
arquivo de dados já conste da pasta criada automaticamente no módulo folder é então realizada a leitura desse
arquivo que está no formato "parquet".

Falar sobre o download das tabelas em formato "dbf"...

Falar sobre o download das tabelas em formato "cnv"...

Esse código é inspirado no projeto de Flávio Coelho (https://github.com/fccoelho/PySUS).

"""

# Função de download de arquivos principais de dados do SIA em formato "dbc" (trata-se de dados...
# das 12 child tables referidas acima, no docstring desse módulo)
def download_SIAXXaamm(base: str, state: str, year: str, month: str, cache: bool=True):

    """
    Downloads a SIA data file in "dbc" format from Datasus ftp server

    :param base: 2 digit character of a SIA child table: PA == Procedimentos Ambulatoriais
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 2 digit character
    :param month: 2 digit character
    :param cache: boolean value
    :return: pandas dataframe object

    """

    state = state.upper()
    fname = f'{base}{state}{year}{month}.dbc'
    cachefile = os.path.join(CACHEPATH, 'SIA_' + fname.split('.')[0] + '_.parquet')

    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    else:
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SIASUS/200801_/Dados/')
        try:
            ftp.retrbinary(f'RETR {fname}', open(CACHEPATH + '\\' + fname, 'wb').write)
        except:
            try:
                ftp.retrbinary(f'RETR {fname.upper()}', open(CACHEPATH + '\\' + fname, 'wb').write)
            except:
                raise Exception(f'Could not access {fname}.')
        df = read_dbc(fname, 'iso-8859-1')
        ftp.close()
        if cache:
            df.to_parquet(cachefile)
        return df


# Função de download de tabelas do SIA em formato "dbf" (trata-se de parent tables)
def download_table_dbf(file_name):

    """
    Fetch a table in "dbf" format from Datasus ftp server
    :param file_name: string of file name without format
    :return: pandas dataframe object

    """

    if ((file_name == 'CADMUN') or (file_name == 'TABUF')):
        fname = file_name + '.DBF'
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS/')
        ftp.retrbinary(f'RETR {fname}', open(fname, 'wb').write)

    elif file_name == 'rl_municip_regsaud':
        folder = 'base_territorial.zip'
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/territorio/tabelas/')
        ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
        zip = ZipFile(folder, 'r')
        fname = file_name + '.dbf'
        zip.extract(fname)

    else:
        folder = 'TAB_SIA.zip'
        if not os.path.isfile(folder):
            ftp = FTP('ftp.datasus.gov.br')
            ftp.login()
            ftp.cwd('/dissemin/publicos/SIASUS/200801_/Auxiliar/')
            ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
        zip = ZipFile(folder, 'r')
        try:
            fname = file_name + '.DBF'
            zip.extract('TAB_DBF/' + fname)
        except:
            try:
                fname = file_name + '.dbf'
                zip.extract('TAB_DBF/' + fname)
            except:
                raise Exception(f'Could not access {file_name}.')

    if ((file_name == 'CADMUN')
        or (file_name == 'TABUF')
        or (file_name == 'rl_municip_regsaud')):
        dbf = DBF(fname)

    else:
        dbf = DBF('TAB_DBF/' + fname, encoding='iso-8859-1')

    df = pd.DataFrame(iter(dbf))

    if ((file_name == 'CADMUN')
        or (file_name == 'TABUF')
        or (file_name == 'rl_municip_regsaud')):
        os.unlink(fname)

    else:
        os.unlink('TAB_DBF/' + fname)

    return df


# Função de download de tabelas do SIA em formato "cnv" (trata-se de parent tables)
def download_table_cnv(file_name):

    """
    Downloads a table in "cnv" format from Datasus ftp server if not already downloaded
    :param file_name: string of file name without format
    :return: pandas dataframe object

    """

    if file_name == 'natjur':
        folder = 'TAB_SIH.zip'
        place = '/dissemin/publicos/SIHSUS/200801_/Auxiliar/'

    else:
        folder = 'TAB_SIA.zip'
        place = '/dissemin/publicos/SIASUS/200801_/Auxiliar/'

    if not os.path.isfile(folder):
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd(place)
        ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
    zip = ZipFile(folder, 'r')
    try:
        fname = file_name + '.CNV'
        zip.extract('CNV/' + fname)
    except:
        try:
            fname = file_name + '.cnv'
            zip.extract('CNV/' + fname)
        except:
            raise Exception(f'Could not access {file_name}.')

    os.rename('CNV/' + fname, 'SIA_' + fname)

    df = read_cnv('SIA_' + fname)

    return df

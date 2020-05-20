###############################################################################################################################################################
# SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN #
###############################################################################################################################################################

import os

import numpy as np
import pandas as pd
from ftplib import FTP
from zipfile import ZipFile
from dbfread import DBF

from .folder import CACHEPATH

if os.name == 'nt':
    from transform.extract.read_windows import read_dbc, read_cnv
elif os.name == 'posix':
    from transform.extract.read_unix import read_dbc, read_cnv

"""
Messy system!!!!!

Lê arquivos principais de dados do SINAN (DENGXXaaaa = Dengue e Chikungunya, XXX) constante do endereço
ftp do Datasus em formato "dbc" como um objeto pandas DataFrame e o salva no formato "parquet". Caso o
arquivo de dados já conste da pasta criada automaticamente no módulo folder é então realizada a leitura
desse arquivo que está no formato "parquet".

Falar sobre o download das tabelas em formato "dbf"...

Falar sobre o download das tabelas em formato "cnv"...

Esse código é baseado no projeto de Flávio Coelho (https://github.com/fccoelho/PySUS).

"""

# Função de download de arquivos principais de dados do SINAN em formato "dbc" (trata-se de dados...
# das n child tables referidas acima, no docstring desse módulo)
def download_SINANXXaa(base: str, state: str, year: str, cache: bool=True):

    """
    Downloads a SINAN data file in "dbc" format from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit character
    :param cache: boolean value
    :return: pandas dataframe object

    """

    state = state.upper()
    fname = f'{base}{state}{year}.dbc'
    cachefile = os.path.join(CACHEPATH, 'SINAN_'+fname.split('.')[0] + '_.parquet')

    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    else:
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        if int('20' + year) <= 2016:
            ftp.cwd('/dissemin/publicos/SINAN/DADOS/FINAIS/')
        elif int('20' + year) == 2017:
            ftp.cwd('/dissemin/publicos/SINAN/DADOS/PRELIM/')
        try:
            ftp.retrbinary(f'RETR {fname}', open(fname, 'wb').write)
        except:
            try:
                ftp.retrbinary(f'RETR {fname.upper()}', open(fname, 'wb').write)
            except:
                raise Exception(f'Could not access {fname}.')
        df = read_dbc(fname, 'iso-8859-1')
        ftp.close()
        if cache:
            df.to_parquet(cachefile)
        return df


# Função de download de tabelas do SINAN em formato "dbf" (trata-se de parent tables)
def download_table_dbf(file_name, cache=True):

    """
    Fetch a table in "dbf" format from Datasus ftp server
    :param file_name: string of file name without format
    :param cache: boolean value
    :return: pandas dataframe object

    """

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    try:
        if file_name == 'CADMUN':
            fname = file_name + '.DBF'
            ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS/')
            ftp.retrbinary(f'RETR {fname}', open(fname, 'wb').write)

        elif file_name == 'rl_municip_regsaud':
            folder = 'base_territorial.zip'
            ftp.cwd('/territorio/tabelas/')
            ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
            zip = ZipFile(folder, 'r')
            fname = file_name + '.dbf'
            zip.extract(fname)
    except:
        raise Exception('Could not download {}'.format(fname))

    dbf = DBF(fname)
    df = pd.DataFrame(iter(dbf))

    os.unlink(fname)

    return df


# Função de download de tabelas do SINAN em formato "cnv" (trata-se de parent tables)
def download_table_cnv(file_name):

    """
    Downloads a table in "cnv" format from Datasus ftp server if not already downloaded
    :param file_name: string of file name without format
    :return: pandas dataframe object

    """

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SINAN/AUXILIAR/')

    folder = 'TAB_SINAN.zip'

    if not os.path.isfile(folder):
        ftp.retrbinary('RETR {}'.format(folder), open(folder, 'wb').write)
    zip = ZipFile(folder, 'r')
    try:
        zip.extract(file_name + '.CNV')
        file_name = file_name + '.CNV'
    except:
        try:
            zip.extract(file_name + '.cnv')
            file_name = file_name + '.cnv'
        except:
            raise Exception('Could not download {}'.format(file_name))

    os.rename(file_name, 'SINAN_' + file_name)

    df = read_cnv('SINAN_' + file_name)

    return df

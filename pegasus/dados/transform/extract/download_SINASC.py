########################################################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC  #
########################################################################################################################################################

import os

import numpy as np
import pandas as pd
from ftplib import FTP
from zipfile import ZipFile
from dbfread import DBF

from transform.extract.folder import CACHEPATH

if os.name == 'nt':
    from transform.extract.read_windows import read_dbc, read_cnv
elif os.name == 'posix':
    from transform.extract.read_unix import read_dbc, read_cnv

"""
Lê arquivos principais de dados do SINASC (DNXXaaaa = Declaração de Nascimento) constante do endereço ftp
do Datasus em formato "dbc" como um objeto pandas DataFrame e o salva no formato "parquet".
Caso o arquivo de dados já conste da pasta criada automaticamente no módulo folder é então realizada
a leitura desse arquivo que está no formato "parquet".

Falar sobre o download das tabelas em formato "dbf"...

Falar sobre o download das tabelas em formato "cnv"...

Esse código é baseado no projeto de Flávio Coelho (https://github.com/fccoelho/PySUS).

"""

# Função de download de arquivos principais de dados do SINASC em formato "dbc" (trata-se de dados...
# da child table referida acima, no docstring desse módulo)
def download_DNXXaaaa(state: str, year: str, cache: bool=True):

    """
    Downloads a DNXXaaaa file in "dbc" format from Datasus ftp server
    :param state: two-letter state identifier: MG == Minas Gerais
    :param year: 4 digit character
    :param cache: boolean value
    :return: pandas dataframe object

    """

    state = state.upper()
    fname = f'DN{state}{year}.dbc'
    cachefile = os.path.join(CACHEPATH, 'SINASC_'+fname.split('.')[0] + '_.parquet')

    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    else:
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SINASC/NOV/DNRES/')
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


# Função de download de tabelas do SINASC em formato "dbf" (trata-se de parent tables)
def download_table_dbf(file_name):

    """
    Fetch a table in "dbf" format from Datasus ftp server
    :param file_name: string of file name without format
    :return: pandas dataframe object

    """

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    fname = file_name + '.DBF'

    try:
        if file_name == 'CNESDN18': # Esse arquivo "dbf" deveria estar no diretório
                                    # ".../dissemin/publicos/SINASC/NOV/TABELAS/" do endereço "ftp"
            ftp.cwd('/dissemin/publicos/SINASC/NOV/TAB/')
            folder = 'NASC_NOV_TAB.zip'
            ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
            zip = ZipFile(folder, 'r')
            zip.extract(fname)

        elif file_name == 'rl_municip_regsaud':
            folder = 'base_territorial.zip'
            ftp.cwd('/territorio/tabelas/')
            ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
            zip = ZipFile(folder, 'r')
            fname = file_name + '.dbf'
            zip.extract(fname)

        else:
            ftp.cwd('/dissemin/publicos/SINASC/NOV/TABELAS/')
            ftp.retrbinary(f'RETR {fname}', open(fname, 'wb').write)
    except:
        raise Exception(f'Could not access {fname}.')

    if ((file_name == 'CNESDN18') or (file_name == 'TABOCUP') or  (file_name == 'CID10')):
        dbf = DBF(fname, encoding='iso-8859-1')
    else:
        dbf = DBF(fname)

    df = pd.DataFrame(iter(dbf))

    os.unlink(fname)

    return df


# Função de download de tabelas do SINASC em formato "cnv" (trata-se de parent tables)
def download_table_cnv(file_name):

    """
    Downloads a table in "cnv" format from Datasus ftp server if not already downloaded
    :param file_name: string of file name without format
    :return: pandas dataframe object

    """

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()

    if (file_name.startswith('CID10') or (file_name == 'NAT1212')):
        ftp.cwd('/dissemin/publicos/SIM/CID10/TAB/')
        folder = 'OBITOS_CID10_TAB.ZIP'
    else:
        ftp.cwd('/dissemin/publicos/SINASC/NOV/TAB/')
        folder = 'NASC_NOV_TAB.zip'

    if not os.path.isfile(folder):
        ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
    zip = ZipFile(folder, 'r')

    try:
        fname = file_name + '.CNV'
        zip.extract(fname)
    except:
        try:
            fname = file_name + '.cnv'
            zip.extract(fname)
        except:
            raise Exception(f'Could not access {file_name}.')

    os.rename(fname, 'SINASC_' + fname)

    df = read_cnv('SINASC_' + fname)

    return df

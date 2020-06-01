###########################################################################################################################
# SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH SIH #
###########################################################################################################################
"""
Realiza o download de arquivos de dados do SIH (RDXXaamm = AIH Reduzidas; SPXXaamm =
AIH Serviços Profissionais) constante do endereço ftp do Datasus em formato "dbc" como
um objeto pandas DataFrame e o salva no formato "parquet". Caso o arquivo de dados já
conste da pasta criada automaticamente no módulo folder é então realizada a leitura
desse arquivo que está no formato "parquet".

Também realiza o download de arquivos auxiliares em formato "dbf" e "cnv".
"""

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
    from transform.extract.read_unix_c import read_dbc, read_cnv


def download_SIHXXaamm(base: str, state: str, year: str, month: str, cache: bool=True):
    """
    Realiza o download de um arquivo principal de dados do SIH em formato "dbc" se já não
    existente no formato "parquet" no diretório CACHEPATH e o lê como um objeto pandas DataFrame

    Parâmetros
    ----------
    base: objeto str
        String de tamanho 2 do nome de uma sub-base de dados do SIH
    state: objeto str
        String de tamanho 2 da sigla de um Estado da RFB
    year: objeto str
        String de tamanho 2 do ano
    month: objeto str
        String de tamanho 2 do mês
    cache: objeto bool
        Boolean se o arquivo "dbc" baixado deve ser salvo no formato "parquet"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados de um arquivo principal de dados originalmente em formato "dbc"
    """

    state = state.upper()
    fname = f'{base}{state}{year}{month}.dbc'
    cachefile = os.path.join(CACHEPATH, 'SIH_' + fname.split('.')[0] + '_.parquet')

    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    else:
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SIHSUS/200801_/Dados/')
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


def download_table_dbf(file_name):
    """
    Realiza o download de um arquivo auxiliar de dados do SIH em formato "dbf" ou de uma pasta
    "zip" que o contém (se a pasta "zip" já não foi baixada), em seguida o lê como um objeto pandas
    DataFrame e por fim o elimina

    Parâmetros
    ----------
    file_name: objeto str
        String do nome do arquivo "dbf"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados de um arquivo auxiliar de dados originalmente em formato "dbf"
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
        folder = 'TAB_SIH.zip'
        if not os.path.isfile(folder):
            ftp = FTP('ftp.datasus.gov.br')
            ftp.login()
            ftp.cwd('/dissemin/publicos/SIHSUS/200801_/Auxiliar/')
            ftp.retrbinary(f'RETR {folder}', open(folder, 'wb').write)
        zip = ZipFile(folder, 'r')
        try:
            fname = file_name + '.DBF'
            zip.extract('DBF/' + fname)
        except:
            try:
                fname = file_name + '.dbf'
                zip.extract('DBF/' + fname)
            except:
                raise Exception(f'Could not access {file_name}.')

    if ((file_name == 'CADMUN')
        or (file_name == 'TABUF')
        or (file_name == 'rl_municip_regsaud')):
        dbf = DBF(fname)

    else:
        dbf = DBF('DBF/' + fname, encoding='iso-8859-1')

    df = pd.DataFrame(iter(dbf))

    if ((file_name == 'CADMUN')
        or (file_name == 'TABUF')
        or (file_name == 'rl_municip_regsaud')):
        os.unlink(fname)

    else:
        os.unlink('DBF/' + fname)

    return df


def download_table_cnv(file_name):
    """
    Realiza o download de um arquivo auxiliar de dados do SIH em formato "cnv" ou de uma pasta
    "zip" que o contém (se a pasta "zip" já não foi baixada), em seguida o lê como um objeto pandas
    DataFrame

    Parâmetros
    ----------
    file_name: objeto str
        String do nome do arquivo "cnv"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados de um arquivo auxiliar de dados originalmente em formato "cnv"
    """

    folder = 'TAB_SIH.zip'

    if not os.path.isfile(folder):
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SIHSUS/200801_/Auxiliar/')
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

    os.rename('CNV/' + fname, 'SIH_' + fname)

    df = read_cnv('SIH_' + fname)

    return df

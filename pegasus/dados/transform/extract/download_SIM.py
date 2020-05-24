###########################################################################################################################
# SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM #
###########################################################################################################################
"""
Realiza o download de arquivos principais de dados do SIM (DOXXaaaa = Declaração de Óbito)
constante do endereço ftp do Datasus em formato "dbc" como um objeto pandas DataFrame e o
salva no formato "parquet". Caso o arquivo de dados já conste da pasta criada automaticamente
no módulo folder é então realizada a leitura desse arquivo que está no formato "parquet".

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


def download_DOXXaaaa(state: str, year: str, cache: bool=True):
    """
    Realiza o download de um arquivo principal de dados do SIM em formato "dbc" se já não
    existente no formato "parquet" no diretório CACHEPATH e o lê como um objeto pandas DataFrame

    Parâmetros
    ----------
    state: objeto str
        String de tamanho 2 da sigla de um Estado da RFB
    year: objeto str
        String de tamanho 4 do ano
    cache: objeto bool
        Boolean se o arquivo "dbc" baixado deve ser salvo no formato "parquet"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados de um arquivo principal de dados originalmente em formato "dbc"
    """

    state = state.upper()
    fname = f'DO{state}{year}.dbc'
    cachefile = os.path.join(CACHEPATH, 'SIM_'+fname.split('.')[0] + '_.parquet')

    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
        return df
    else:
        ftp = FTP('ftp.datasus.gov.br')
        ftp.login()
        ftp.cwd('/dissemin/publicos/SIM/CID10/DORES/')
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
    Realiza o download de um arquivo auxiliar de dados do SIM em formato "dbf" ou de uma pasta
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

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    fname = file_name + '.DBF'

    try:
        if file_name == 'CNESDO18': # Esse arquivo "dbf" deveria estar no diretório
                                    # ".../dissemin/publicos/SIM/CID10/TABELAS" do endereço "ftp"
            folder = 'OBITOS_CID10_TAB.ZIP'
            ftp.cwd('/dissemin/publicos/SIM/CID10/TAB/')
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
            ftp.cwd('/dissemin/publicos/SIM/CID10/TABELAS/')
            ftp.retrbinary(f'RETR {fname}', open(fname, 'wb').write)
    except:
        raise Exception(f'Could not access {fname}.')

    if ((file_name == 'TABOCUP') or (file_name == 'CNESDO18') or (file_name == 'CID10')):
        dbf = DBF(fname, encoding='iso-8859-1')
    else:
        dbf = DBF(fname)

    df = pd.DataFrame(iter(dbf))

    os.unlink(fname)

    return df


def download_table_cnv(file_name):
    """
    Realiza o download de um arquivo auxiliar de dados do SIM em formato "cnv" ou de uma pasta
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

    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIM/CID10/TAB/')

    folder = 'OBITOS_CID10_TAB.ZIP'

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

    os.rename(fname, 'SIM_' + fname)

    df = read_cnv('SIM_' + fname)

    return df

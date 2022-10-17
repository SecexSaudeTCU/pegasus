"""
Módulo que contém funções que leem arquivo "dbc" em plataformas Windows utilizando o programa "dbf2dbc.exe"
e arquivo texto em formato "cnv".
"""

import os
import subprocess
from pathlib import Path

from dbfread import DBF
import pandas as pd


def dbc2dbf(infile):
    """
    Descompacta um arquivo "dbc" para "dbf" utilizando o programa executável "dbf2dbc" pertencente ao Datasus.

    Parâmetros
    ----------
    infile: objeto str
        String do nome do arquivo "dbc"

    Retorno
    -------
    outfile: objeto str
        String do nome do arquivo "dbf"
    """

    subprocess.run(['C:/TabWin/dbf2dbc.exe', infile])
    os.unlink(infile)
    outfile = infile[:-4] + '.dbf'

    return outfile


def read_dbc(filename, signature='utf-8'):
    """
    Descompacta um arquivo "dbc" para "dbf", em seguida o lê como tal e por fim o converte em um objeto
    pandas DataFrame e o elimina.

    Parâmetros
    ----------
    filename: objeto str
        String do nome do arquivo "dbc"
    signature: objeto str
        String do nome do formato de encoding do arquivo "dbc"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados de um arquivo principal de dados originalmente em formato "dbc"
    """

    file_name = dbc2dbf(filename)
    dbf = DBF(file_name, encoding=signature)
    df = pd.DataFrame(iter(dbf))
    os.unlink(file_name)

    return df


def attempt_int(value):
    """
    Converte um objeto referenciado pelo parâmetro "value" para o tipo int ou caso não seja possível
    o retorna íntegro.

    Parâmetros
    ----------
    infile: objeto de qualquer tipo
        Objeto que se deseja converter para o tipo int

    Retorno
    -------
    df: objeto int ou na sua class original
        Objeto convertido para o tipo int ou íntegro
    """

    try:
        return int(value)
    except (ValueError, TypeError):
        return value


def read_cnv(filename):
    """
    Converte o conteúdo útil do arquivo texto em formato "cnv" para um objeto pandas DataFrame.

    Parâmetros
    ----------
    filename: objeto str
        String do nome do arquivo "cnv"

    Retorno
    -------
    df: objeto pandas DataFrame
        Dataframe que contém os dados úteis de um arquivo auxiliar de dados em formato "cnv"
    """

    # Cria um file handler do arquivo "cnv"
    fhandler = open(filename)

    k = 0  # Inicialização de uma variável para identificar a primeira e as outras linhas
    lista_significacao = []  # "Inicialização" da lista de decodificações
    lista_id = []  # "Inicialização" da lista de códigos
    # Leitura do arquivo texto
    if filename[:-4] in ['SINAN_Classdeng', 'CNV/CNES_NATUREZA']:   # Estes são arquivos "cnv" cujos números de algarismos dos códigos estão na segunda linha do arquivo
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Passa a primeira linha
            if k == 1:
                continue
                # Coleta na segunda linha do arquivo o número de caracteres dos códigos
            elif k == 2:
                size_id = int(linha.split()[1])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].strip())
                lista_id.append(linha[60:60+size_id].strip())

    elif filename[:-4] in ['CNV/CNES_TP_ESTAB', 'CNV/CNES_NATJUR', 'SIH_LEITOS']:  # Estes são arquivos "cnv" cujas significações não começam na posição (python) 9...
                                                              # (mas na 11), cujos códigos não começam na posição (python) 60 (mas na 112) de...
                                                              # cada linha do arquivo, e cujos números de algarismos dos códigos não são o segundo...
                                                              # elemento listado na primeira linha (mas o terceiro)
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Coleta na primeira linha do arquivo o número de caracteres dos códigos
            if k == 1:
                size_id = int(linha.split()[2])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[11:112].strip())
                lista_id.append(linha[112:112+size_id].strip())

    elif filename[:-4] == 'CNV/CNES_Equip_Tp':  # Os dois últimos dígitos da coluna de códigos desse arquivo permite construir a parent table CODEQUIP e assim...
                                            # decodificar a coluna CODEQUIP_ID da tabela eqbr do banco de dados CNES_EQ
        size_id = 2
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # O número de caracteres dos códigos já foi definido acima
            if k == 1:
                continue
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].strip())
                lista_id.append(linha[61:61+size_id].strip())

    elif filename[:-4] == 'SIA_CARAT_AT': # Estes são arquivos "cnv" cujos números de algarismos dos códigos não estão na terceira linha do arquivo
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Passa a primeira linha
            if (k == 1) or (k == 2):
                continue
                # Coleta na terceira linha do arquivo o número de caracteres dos códigos
            elif k == 3:
                size_id = int(linha.split()[1])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].strip())
                lista_id.append(linha[60:60+size_id].strip())

    else: # Arquivos "cnv" padrão
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Coleta na primeira linha do arquivo o número de caracteres dos códigos
            if k == 1:
                size_id = int(linha.split()[1])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].strip())
                lista_id.append(linha[60:60+size_id].strip())

    # "Inicialização" de um objeto pandas DataFrame com as colunas ID e SIGNIFICACAO
    df = pd.DataFrame(columns=['ID', 'SIGNIFICACAO'])
    # Transferência dos dados da lista de códigos para a coluna ID do objeto pandas DataFrame
    df['ID'] = lista_id
    # Transferência dos dados da lista de decodificações para a coluna SIGNIFICACAO do objeto pandas DataFrame
    df['SIGNIFICACAO'] = lista_significacao
    # Remove o arquivo "cnv" após fechar o "fhandler"
    fhandler.close()
    os.remove(filename)
    # Torna UPPER CASE todos os valores da coluna SIGNIFICACAO
    df['SIGNIFICACAO'] = df['SIGNIFICACAO'].apply(lambda x: x.upper())
    # Modifica o valor tipo string da célula da coluna SIGNIFICACAO que contém o valor Ign
    df['SIGNIFICACAO'].replace('IGN', 'IGNORADO', inplace=True)
    df = df.drop(df[df['SIGNIFICACAO']=='IGNORADO'].index)
    # Elimina eventual linha com string impertinente na coluna ID
    df = df.drop(df[df['ID']==''].index)
    df = df.drop(df[df['ID']==','].index)

    # Elimina duplicates do objeto pandas DataFrame com base na coluna ID
    df.drop_duplicates(subset='ID', keep='first', inplace=True)

    if (len(df.loc[df['ID'].astype(str).str.isdigit(), 'ID'].tolist()) == df.shape[0]) or (len(df.loc[~df['ID'].astype(str).str.isdigit(), 'ID'].tolist()) == df.shape[0]):
        # Converte de string para int cada valor da coluna de códigos do objeto pandas DataFrame se possível
        df['ID'] = df['ID'].apply(lambda x: attempt_int(x))
        # Ordena o objeto pandas DataFrame com base na coluna ID
        df.sort_values(by=['ID'], inplace=True)

    # Converte de int para string cada valor da coluna de códigos do objeto pandas DataFrame
    df['ID'] = df['ID'].apply(lambda x: str(x))

    # Reset o index do objeto pandas DataFrame devido ao sorting e ao droping evetualmente efetuados
    df.reset_index(drop=True, inplace=True)
    return df

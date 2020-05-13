import os
import subprocess
import codecs
from tempfile import NamedTemporaryFile

from dbfread import DBF
import pandas as pd

from .folder import CACHEPATH
from ._readdbc import ffi, lib

"""
The functions read_dbc and dbc2dbf are based on the work of Flávio Coelho (https://github.com/fccoelho/PySUS)

"""

def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new('char[]', os.path.abspath(infile))
    q = ffi.new('char[]', os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])


def read_dbc(filename, signature='utf-8'):
    """
    Opens a DATASUS .dbc file and return its contents as a pandas
    Dataframe.
    :param filename: .dbc filename
    :param encoding: encoding of the data
    :return: Pandas Dataframe.
    """
    if isinstance(filename, str):
        filename = filename.encode()
    with NamedTemporaryFile(delete=False) as tf:
        dbc2dbf(filename, tf.name.encode())
        dbf = DBF(tf.name, encoding=signature)
        df = pd.DataFrame(list(dbf))
    os.unlink(tf.name)

    return df


# Função que converte o conteúdo útil (!!!) do arquivo texto em formato "cnv" para um objeto pandas DataFrame

# Às vezes os arquivos "cnv" vem com mais de um código, separados por vírgula, para uma mesma decodificação. O script abaixo coleta o primeiro código
# que é o que vale para aquela decodificação.

# Nesse caso, pode constar do arquivo de dados principal (no caso do SINASC: DNXXaaaa; no caso do SIM: DOXXaaaa, etc.) valores antigos de código. Isto é,
# valores que constam após a vírgula na "coluna" código do arquivo "cnv" (a última "coluna"). Assim, no passo de preprocessing dos dados, torna-se
# necessário usar o método replace na coluna do objeto pandas DataFrame (foreign key) para substituir o(s) valor(es) antigos pelo atual.

# Veja o exemplo do comando replace para a coluna CODANOMAL (foreign key) do arquivo de dados principal do SINASC (DNXXaaaa) por conter valor antigo para
# um código de doença discriminado na respectiva tabela relacional representada pelo arquivo CID1017.cnv:

# Código python::: df['CODANOMAL'].replace(['Q356', 'Q358'], 'Q359', inplace=True)
# Linha do arquivo CID1017.cnv::: 213  Q35.9 Fenda palatina NE                            Q359,Q356,Q358,


# Função para converter um "value" no type "int" ou caso não seja possível utiliza o valor "default"
def attempt_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


def read_cnv(name_of_file):

    """
    Opens a Datasus "cnv" file and return its contents as a pandas
    Dataframe object.
    :param filename: file name without format
    :return: pandas Dataframe object.

    """

    # Cria um file handler do arquivo "cnv"
    try:
        # Torna a variável string "name_of_file" UPPER CASE e concatena a ela a string ".cnv"
        filename = name_of_file[:-4] + '.cnv'
        # Cria um file handler do arquivo "cnv"
        fhandler = codecs.open(filename, 'r', encoding='iso-8859-1', errors='ignore')
    except:
        # Torna a variável string "name_of_file" UPPER CASE e concatena a ela a string ".CNV"
        filename = name_of_file[:-4] + '.CNV'
        # Cria um file handler do arquivo "CNV"
        fhandler = codecs.open(filename, 'r', encoding='iso-8859-1', errors='ignore')

    k = 0  # Inicialização de uma variável para identificar a primeira e as outras linhas
    lista_significacao = []  # "Inicialização" da lista de decodificações
    lista_id = []  # "Inicialização" da lista de códigos
    # Leitura do arquivo texto
    if filename[:-4] in ['SINAN_Classdeng', 'cnv/CNES_NATUREZA']:   # Estes são arquivos "cnv" cujos números de algarismos dos códigos estão na segunda linha do arquivo
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

    elif filename[:-4] in ['cnv/CNES_TP_ESTAB', 'cnv/CNES_NATJUR', 'SIH_LEITOS']:  # Estes são arquivos "cnv" cujas significações não começam na posição (python) 9...
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

    elif filename[:-4] == 'cnv/CNES_Equip_Tp':  # Os dois últimos dígitos da coluna de códigos desse arquivo permite construir a parent table CODEQUIP e assim...
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

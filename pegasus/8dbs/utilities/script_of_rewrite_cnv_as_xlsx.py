import pandas as pd

"""
Convert from the crappy format "cnv" (already downloaded in a non-zipped folder) to "xlsx" as a last step.

"""

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


# Função que converte o conteúdo útil (!!!) do arquivo texto em formato "cnv" para um objeto pandas DataFrame
def df_from_cnv(path, name_of_file):
    # Torna a variável string "name_of_file" UPPER CASE e concatena a ela a string ".cnv"
    filename = name_of_file + '.cnv'
    # Cria um file handler do arquivo "cnv"
    fhandler = open(path + filename)

    k = 0  # Inicialização de um variável para identificar a primeira e as outras linhas
    lista_significacao = []  # "Inicialização" da lista de decodificações
    lista_id = []  # "Inicialização" da lista de códigos

    # Leitura do arquivo texto
    if filename[:-4] == 'CLASSDENG':   # Esse arquivo "cnv" pertence ao SINAN_DENG
                                       # Único arquivo "cnv" (até o momento) cujo número de algarismos dos códigos não estão na primeira linha do arquivo
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Passa a primeira linha
            if k == 1:
                continue
                # Coleta na primeira linha do arquivo o número de caracteres dos códigos
            elif k == 2:
                size_id = int(linha.split()[1])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].strip())
                lista_id.append(linha[60:60+size_id].strip())

    # Leitura do arquivo texto
    elif filename[:-4] == 'TP_ESTAB':  # Esse arquivo "cnv" pertence ao CNES_ST
                                       # Único arquivo "cnv" (até o momento) cujos códigos não começam na posição 60 (mas na 112) de cada linha do...
                                       # arquivo e cujo número de algarismos dos códigos não são o segundo elemento listado na primeira linha (mas o terceiro)
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Coleta na primeira linha do arquivo o número de caracteres dos códigos
            if k == 1:
                size_id = int(linha.split()[2])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[11:112].strip())
                lista_id.append(linha[112:122+size_id].strip())

    # Leitura do arquivo texto
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
    df_object = pd.DataFrame(columns=['ID', 'SIGNIFICACAO'])
    # Transferência dos dados da lista de códigos para a coluna ID do objeto pandas DataFrame
    df_object['ID'] = lista_id
    # Transferência dos dados da lista de decodificações para a coluna SIGNIFICACAO do objeto pandas DataFrame
    df_object['SIGNIFICACAO'] = lista_significacao

    # Torna UPPER CASE todos os valores da coluna SIGNIFICACAO
    df_object['SIGNIFICACAO'] = df_object['SIGNIFICACAO'].apply(lambda x: x.upper())
    # Modifica o valor tipo string da célula da coluna SIGNIFICACAO que contém o valor IGN
    df_object['SIGNIFICACAO'].replace('IGN', 'IGNORADO', inplace=True)
    df_object = df_object.drop(df_object[df_object['SIGNIFICACAO']=='IGNORADO'].index)

    # Elimina eventual linha com string vazia na coluna ID
    df_object = df_object.drop(df_object[df_object['ID']==''].index)
    df_object = df_object.drop(df_object[df_object['ID']==','].index)

    # Converte de string para int cada valor da coluna de códigos do objeto pandas DataFrame se possível
    df_object['ID'] = df_object['ID'].apply(lambda x: attempt_int(x))

    # Elimina duplicates do objeto pandas DataFrame com base na coluna ID
    df_object.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena o objeto pandas DataFrame com base na coluna ID
    df_object.sort_values(by=['ID'], inplace=True)
    # Reset o index do objeto pandas DataFrame devido ao passo anterior
    df_object.reset_index(drop=True, inplace=True)
    return df_object


# Função que converte o conteúdo útil (!!!) dos arquivos textos do CID10 em formato "cnv" para um objeto pandas DataFrame
def df_from_CID10_cnv(path):
    frames = []
    for i in range(1, 22):
        i = str(i).zfill(2)
        filename = 'CID10_' + i + '.cnv'
        # Cria um file handler do arquivo "cnv"
        fhandler = open(path + filename)

        k = 0  # Inicialização de um variável para identificar a primeira e as outras linhas
        lista_significacao = []  # "Inicialização" da lista de decodificações
        lista_id = []  # "Inicialização" da lista de códigos
        # Leitura do arquivo texto
        for linha in fhandler:
            k += 1
            linha = linha.rstrip()
            # Coleta na primeira linha do arquivo o número de caracteres dos códigos
            if k == 1:
                size_id = int(linha.split()[1])
            # Coleta nas outras linhas do arquivo a decodificação e o respectivo código, respectivamente, e append o elemento coletado às respectivas listas
            else:
                lista_significacao.append(linha[9:60].rstrip())
                lista_id.append(linha[60:60+size_id].rstrip())
        # "Inicialização" de um objeto pandas DataFrame com as colunas ID e SIGNIFICACAO
        df_object = pd.DataFrame(columns=['ID', 'SIGNIFICACAO'])
        # Transferência dos dados da lista de códigos para a coluna ID do objeto pandas DataFrame
        df_object['ID'] = lista_id
        # Transferência dos dados da lista de decodificações para a coluna SIGNIFICACAO do objeto pandas DataFrame
        df_object['SIGNIFICACAO'] = lista_significacao
        # Elimina eventual linha com string vazia na coluna ID
        df_object = df_object.drop(df_object[df_object['ID']==''].index)
        df_object = df_object.drop(df_object[df_object['ID']==','].index)
        # Elimina duplicates do objeto pandas DataFrame com base na coluna ID
        df_object.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena o objeto pandas DataFrame com base na coluna ID
        df_object.sort_values(by=['ID'], inplace=True)
        # Reset o index do objeto pandas DataFrame devido ao passo anterior
        df_object.reset_index(drop=True, inplace=True)
        frames.append(df_object)
    dfinal = pd.concat(frames, ignore_index=True)
    dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
    dfinal.sort_values(by=['ID'], inplace=True)
    dfinal.reset_index(drop=True, inplace=True)
    return dfinal



# Entrada do nome do arquivo "cnv"
file_name = input('Enter with the CNV file name (maybe lower case but necessarily without extension): ').upper()

if file_name.startswith('CID10'):
    print('Better to use the CID10 files from SIM even if the related database is another one!')
    print('The files were downloaded from ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/TAB/')
    base = 'SIM'
    # Pasta onde estão os arquivos auxiliares de tabulação dos 21 capítulos do CID10 baixados de ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/TAB/
    path1 = 'H:\\Atuando\\NTDI\\Data\\Base_de_Dados\\SIM\\Arquivos_Auxiliares_de_Tabulacao\\'
    df = df_from_CID10_cnv(path1)
else:
    base = (input('Enter with the Datasus database name (SIM/SINASC/CNES/SINAN) (maybe lower case): ')).upper()
    # Pasta onde estão os arquivos auxiliares de tabulação baixados de ftp://ftp.datasus.gov.br/
    path1 = 'H:\\Atuando\\NTDI\\Data\\Base_de_Dados\\' + base + '\\Arquivos_Auxiliares_de_Tabulacao\\'
    df = df_from_cnv(path1, file_name)

# Ajustar "path2" para onde deseja salvar o arquivo "xlsx" gerado a partir do arquivo em formato "cnv"
path2 =  'C:\\Users\\ericc\\Desktop\\'

# Conversão de um objeto pandas DataFrame para um arquivo "xlsx"
excel_writer = pd.ExcelWriter(path2 + 'TCC_' + file_name + '.xlsx')
df.to_excel(excel_writer, index = False)
excel_writer.save()
print(df)

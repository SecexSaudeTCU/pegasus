###########################################################################################################################
# SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA #
###########################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from transform.extract.download_SIA import download_SIAXXaamm, download_table_dbf, download_table_cnv

"""
Módulo de limpeza/tratamento de dados do SIA.

"""

# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Classe de dados principais do SIA
class DataSiaMain:

    # Construtor
    def __init__(self, base, state, year, month):
        self.base = base
        self.state = state
        self.year = year
        self.month = month


    # Método para ler como um objeto pandas DataFrame um arquivo principal de dados do SIA e adequar e formatar suas...
    # colunas e valores
    def get_SIAXXaamm_treated(self):
        # Lê o arquivo "dbc" ou "parquet", se já tiver sido baixado, como um objeto pandas DataFrame
        dataframe = download_SIAXXaamm(self.base, self.state, self.year, self.month)
        print(f'O número de linhas do arquivo {self.base}{self.state}{self.year}{self.month} é {dataframe.shape[0]}.')

        ###################################################################################################################
        # SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
        ###################################################################################################################
        if self.base == 'PA':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela pabr da base de dados
            lista_columns = np.array(['PA_CODUNI', 'PA_GESTAO', 'PA_CONDIC', 'PA_UFMUN', 'PA_REGCT', 'PA_INCOUT',
                                      'PA_INCURG', 'PA_TPUPS', 'PA_TIPPRE', 'PA_MN_IND', 'PA_CNPJCPF', 'PA_CNPJMNT',
                                      'PA_CNPJ_CC', 'PA_MVM', 'PA_CMP', 'PA_PROC_ID', 'PA_TPFIN', 'PA_NIVCPL',
                                      'PA_DOCORIG', 'PA_AUTORIZ', 'PA_CNSMED', 'PA_CBOCOD', 'PA_MOTSAI', 'PA_OBITO',
                                      'PA_ENCERR', 'PA_PERMAN', 'PA_ALTA', 'PA_TRANSF', 'PA_CIDPRI', 'PA_CIDSEC',
                                      'PA_CIDCAS', 'PA_CATEND', 'PA_IDADE', 'IDADEMIN', 'IDADEMAX', 'PA_FLIDADE',
                                      'PA_SEXO', 'PA_RACACOR', 'PA_MUNPCN', 'PA_QTDPRO', 'PA_QTDAPR', 'PA_VALPRO',
                                      'PA_VALAPR', 'PA_UFDIF', 'PA_MNDIF', 'PA_DIF_VAL', 'NU_VPA_TOT', 'NU_PA_TOT',
                                      'PA_INDICA', 'PA_CODOCO', 'PA_FLQT', 'PA_FLER', 'PA_ETNIA', 'PA_VL_CF',
                                      'PA_VL_CL', 'PA_VL_INC', 'PA_SRC_C', 'PA_INE', 'PA_NAT_JUR'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão...
            # presentes na variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Inserção de coluna de grupo, subgrupo e forma do procedimento realizado
            df['GRUPO'] = df['PA_PROC_ID'].apply(lambda x: x[:2])
            df['SUBGRUPO'] = df['PA_PROC_ID'].apply(lambda x: x[:4])
            df['FORMA'] = df['PA_PROC_ID'].apply(lambda x: x[:6])


            # Simplifica/corrige a apresentação dos dados das colunas especificadas
            df['PA_CODUNI'] = df['PA_CODUNI'].apply(lambda x: x.zfill(7))
            df['PA_CODUNI'] = df['PA_CODUNI'].apply(str.strip)
            df['PA_CODUNI'] = df['PA_CODUNI'].apply(lambda x: x if len(x) == 7 else '')

            for col in np.array(['PA_REGCT', 'PA_INCOUT', 'PA_INCURG']):
                df[col] = df[col].apply(lambda x: x.zfill(4))
                df[col] = df[col].apply(str.strip)

            for col in np.array(['PA_TPUPS', 'PA_TPFIN', 'PA_CATEND', 'PA_RACACOR']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            df['PA_AUTORIZ'].replace(to_replace=',', value= '', regex=True, inplace=True)

            for col in np.array(['PA_CBOCOD', 'PA_SRC_C']):
                df[col] = df[col].apply(lambda x: x.zfill(6))
                df[col] = df[col].apply(str.strip)
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')

            for col in np.array(['PA_CIDPRI', 'PA_CIDSEC', 'PA_CIDCAS']):
                df[col] = df[col].apply(lambda x: x[:4] if len(x) > 4 else x)

            df['PA_CODOCO'] = df['PA_CODOCO'] + df['PA_FLQT'] # Fusão em uma coluna
            df = df.drop(['PA_FLQT'], axis=1) # Drop a coluna PA_FLQT

            df['PA_ETNIA'] = df['PA_ETNIA'].apply(lambda x: x.zfill(4))
            df['PA_ETNIA'] = df['PA_ETNIA'].apply(str.strip)
            df['PA_ETNIA'] = df['PA_ETNIA'].apply(lambda x: x if len(x) == 4 else '')

            # Atualiza/corrige os labels das colunas especificadas
            for col in np.array(['PA_GESTAO', 'PA_UFMUN', 'PA_MUNPCN']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
                df[col].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
                df[col].replace(['000000', '150475', '421265', '422000', '431454',
                                 '500627', '510445', '999999'], '', inplace=True)
                df[col].replace(['530000', '530020', '530030', '530040', '530050',
                                 '530060', '530070', '530080', '530090', '530100',
                                 '530110', '530120', '530130', '530135', '530140',
                                 '530150', '530160','530170', '530180'] + \
                                 [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['PA_REGCT'].replace(['7114'], '', inplace=True)

            for col in np.array(['PA_INCOUT', 'PA_INCURG']):
                df[col].replace(['0001', '9999'], '1', inplace=True) # "1" de "Sim"
                df[col].replace('0000', '0', inplace=True)           # "0" de "Não"

            df['PA_TPUPS'].replace(['82', '84', '85', '99'], '', inplace=True)

            df['PA_TIPPRE'].replace('00', '', inplace=True)

            for col in np.array(['PA_MVM', 'PA_CMP']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            for col in np.array(['PA_CBOCOD', 'PA_SRC_C']):
                df[col].replace(['000000', '515140', '5151F1'], '', inplace=True)

            df['PA_MOTSAI'].replace('00', '0', inplace=True)

            for col in np.array(['PA_CIDPRI', 'PA_CIDSEC', 'PA_CIDCAS']):
                df[col] = df[col].apply(lambda x: x.upper())
                df[col].replace(['0000', '9999', 'C200', 'C550', 'C560', 'C610', 'C640',
                                 'C800', 'C970', 'G455', 'J460', 'J462', 'J464', 'J465',
                                 'J466', 'J467', 'J470', 'J481', 'J484', 'R007', 'R495'], '', inplace=True)
                df[col].replace('B501', 'B508', inplace=True)
                df[col].replace('B656', 'B653', inplace=True)
                df[col].replace('C141', 'C140', inplace=True)
                df[col].replace('M723', 'M724', inplace=True)
                df[col].replace('M725', 'M728', inplace=True)
                df[col].replace('N975', 'N978', inplace=True)
                df[col].replace('Q314', 'P288', inplace=True)
                df[col].replace('Q350', 'Q351', inplace=True)
                df[col].replace('Q352', 'Q353', inplace=True)
                df[col].replace('Q354', 'Q355', inplace=True)
                df[col].replace(['Q356', 'Q358'], 'Q359', inplace=True)
                df[col].replace('R500', 'R508', inplace=True)
                df[col].replace('R501', 'R500', inplace=True)
                df[col].replace(['X590', 'X591', 'X592', 'X593', 'X594', 'X595',
                                 'X596', 'X597', 'X598'], 'X599', inplace=True)
                df[col].replace('Y34', 'Y349', inplace=True)
                df[col].replace('Y447', 'Y448', inplace=True)
                df[col].replace('U04', '', inplace=True)

            df['PA_CATEND'].replace(['0', '7', '10', '12', '20', '53', '54',
                                     '57', '0-', '0E', '0U'], '', inplace=True)

            df['PA_RACACOR'].replace(['0', '6', '9', '1M', '1G', '1C',
                                      'DE', 'D', '87', '99'], '', inplace=True)

            df['PA_ETNIA'].replace(['0000', '9999'], '', inplace=True)
            df['PA_ETNIA'].replace(regex='^X.{3}', value='', inplace=True)

            df['PA_INE'].replace(['0001610902', '0001619055', '0001630520',
                                  '0001627112', '0001636383'], '', inplace=True)

            df['PA_NAT_JUR'].replace(['0000', '1244'], '', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['PA_CODUNI', 'PA_GESTAO', 'PA_CONDIC', 'PA_UFMUN', 'PA_REGCT',
                                 'PA_TPUPS', 'PA_TIPPRE',  'PA_PROC_ID', 'PA_TPFIN', 'PA_NIVCPL',
                                 'PA_DOCORIG', 'PA_CBOCOD', 'PA_MOTSAI', 'PA_CIDPRI', 'PA_CIDSEC',
                                 'PA_CIDCAS', 'PA_CATEND', 'PA_FLIDADE', 'PA_SEXO', 'PA_RACACOR',
                                 'PA_MUNPCN', 'PA_INDICA', 'PA_CODOCO', 'PA_ETNIA', 'PA_SRC_C',
                                 'PA_INE', 'PA_NAT_JUR', 'GRUPO', 'SUBGRUPO', 'FORMA']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['PA_MN_IND', 'PA_CNPJCPF', 'PA_CNPJMNT',
                                 'PA_CNPJ_CC', 'PA_AUTORIZ', 'PA_CNSMED']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['PA_MVM', 'PA_CMP']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['PA_MVM', 'PA_CMP']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários...
            # (0 ou 1) ou quando simplesmente se deseja inteiros
            for col in np.array(['PA_INCOUT', 'PA_INCURG', 'PA_OBITO', 'PA_ENCERR', 'PA_PERMAN', 'PA_ALTA', 'PA_TRANSF',
                                 'PA_IDADE', 'IDADEMIN', 'IDADEMAX', 'PA_QTDPRO', 'PA_QTDAPR', 'PA_UFDIF', 'PA_FLER']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float com duas casas decimais as colunas de atributos de valores...
            # representativos de quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['PA_VALPRO', 'PA_VALAPR', 'PA_DIF_VAL', 'NU_VPA_TOT',
                                 'NU_PA_TOT', 'PA_VL_CF','PA_VL_CL', 'PA_VL_INC']):
                df[col] = df[col].apply(lambda x: round(float(x), 2) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'PA_CODUNI': 'PACODUNI_ID', 'PA_GESTAO': 'PAGESTAO_ID',
                                          'PA_CONDIC': 'PACONDIC_ID', 'PA_UFMUN': 'PAUFMUN_ID',
                                          'PA_REGCT': 'PAREGCT_ID', 'PA_TPUPS': 'PATPUPS_ID',
                                          'PA_TIPPRE': 'PATIPPRE_ID', 'PA_PROC_ID': 'PAPROC_ID',
                                          'PA_TPFIN': 'PATPFIN_ID', 'PA_NIVCPL': 'PANIVCPL_ID',
                                          'PA_DOCORIG': 'PADOCORIG_ID', 'PA_CBOCOD': 'PACBOCOD_ID',
                                          'PA_MOTSAI': 'PAMOTSAI_ID', 'PA_CIDPRI': 'PACIDPRI_ID',
                                          'PA_CIDSEC': 'PACIDSEC_ID', 'PA_CIDCAS': 'PACIDCAS_ID',
                                          'PA_CATEND': 'PACATEND_ID', 'PA_FLIDADE': 'PAFLIDADE_ID',
                                          'PA_SEXO': 'PASEXO_ID', 'PA_RACACOR': 'PARACACOR_ID',
                                          'PA_MUNPCN': 'PAMUNPCN_ID', 'PA_INDICA': 'PAINDICA_ID',
                                          'PA_CODOCO': 'PACODOCO_ID', 'PA_ETNIA': 'PAETNIA_ID',
                                          'PA_SRC_C': 'PASRCC_ID', 'PA_INE': 'PAINE_ID',
                                          'PA_NAT_JUR': 'PANATJUR_ID', 'GRUPO': 'GRUPO_ID',
                                          'SUBGRUPO': 'SUBGRUPO_ID', 'FORMA': 'FORMA_ID'}, inplace=True)

            print(f'Tratou o arquivo PA{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df


# Classe de dados auxiliares do SIA
class DataSiaAuxiliary:

    # Construtor
    def __init__(self, path):
        self.path = path


    #######################################################################################################################
    # SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA SIA #
    #######################################################################################################################


    ###################################################################################################################
    # SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
    ###################################################################################################################
    # Função para adequar e formatar as colunas e valores das 27 Tabelas CADGERXX (arquivos CADGERXX.dbf),...
    # sendo uma para cada estado do Brasil
    def get_CADGERBR_treated(self):
        # Conversão das 27 Tabelas CADGERXX para um objeto pandas DataFrame
        frames = []
        lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                         'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
        for state in lista_estados:
            file_name = 'CADGER' + state
            dfi = download_table_dbf(file_name)
            # Renomeia as colunas especificadas
            dfi.rename(index=str, columns={'CNES': 'ID', 'FANTASIA': 'DESCESTAB'}, inplace=True)
            # Remove colunas indesejáveis do objeto pandas DataFrame
            dfi = dfi.drop(['RAZ_SOCI', 'LOGRADOU', 'NUM_END', 'COMPLEME', 'BAIRRO',
                            'COD_CEP', 'TELEFONE',  'FAX', 'EMAIL', 'REGSAUDE', 'MICR_REG',
                            'DISTRSAN', 'DISTRADM', 'CODUFMUN', 'NATUREZA'], axis=1)
            # Reordena as colunas
            dfi = dfi[['ID', 'DESCESTAB', 'RSOC_MAN', 'CPF_CNPJ', 'EXCLUIDO', 'DATAINCL', 'DATAEXCL']]
            # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
            dfi.drop_duplicates(subset='ID', keep='first', inplace=True)
            # Ordena as linhas por ordem crescente dos valores da coluna ID
            dfi.sort_values(by=['ID'], inplace=True)
            # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
            dfi.reset_index(drop=True, inplace=True)
            frames.append(dfi)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
        df['EXCLUIDO'] = df['EXCLUIDO'].apply(lambda x: tryconvert(x, None, int))
        # Substitui o objeto datetime.date "9999-12-31" das duas colunas de "datas" pelo objeto...
        # datetime.date "2099-01-01"
        for col in np.array(['DATAINCL', 'DATAEXCL']):
            df[col].replace(datetime(9999, 12, 31).date(), datetime(2099, 1, 1).date(), inplace=True)
        # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
        # data futura "2099-01-01"
        for col in np.array(['DATAINCL', 'DATAEXCL']):
            df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                    datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

        # Upload do arquivo "xlsx" que contém os CNES presentes nos arquivos PAXXaamm (dos anos de...
        # 2008 a 2019) e não presentes nas 27 Tabelas CADGERXX. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'PA_CODUNI_OUT_CADGER_XX_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até...
        # formar uma "string" de tamanho = 7
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(7))
        # Adiciona as colunas DESCESTAB, RSOC_MAN, CPF_CNPJ, EXCLUIDO, DATAINCL e DATAEXCL e respectivos...
        # valores ao objeto "dataframe"
        dataframe['DESCESTAB'] = 'NAO PROVIDO EM 27 ARQUIVOS DBF DE CNES'
        dataframe['RSOC_MAN'] = '?'
        dataframe['CPF_CNPJ'] = '?'
        dataframe['EXCLUIDO'] = None
        dataframe['DATAINCL'] = datetime(2099, 1, 1).date()
        dataframe['DATAEXCL'] = datetime(2099, 1, 1).date()
        # Concatenação do objeto "dataframe" ao objeto "df"
        frames = []
        frames.append(df)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', None, datetime(2099, 1, 1), datetime(2099, 1, 1)]
        return dfinal


    # Método para adequar e formatar as colunas e valores da Tabela CADMUN (arquivo CADMUN.dbf)
    def get_CADMUN_treated(self):
        # Conversão da Tabela CADMUN para um objeto pandas DataFrame
        file_name = 'CADMUN'
        df1 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'MUNCOD': 'ID', 'UFCOD': 'UFCOD_ID'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nenhum município
        df1 = df1.drop(df1[df1['ID']=='000000'].index)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df1 = df1.drop(['MUNSINON', 'MUNSINONDV', 'MESOCOD', 'MICROCOD', 'MSAUDCOD',
                        'RSAUDCOD', 'CSAUDCOD', 'RMETRCOD', 'AGLCOD'], axis=1)
        # Substitui uma string vazia pela string "?" nas colunas especificadas
        for col in ['SITUACAO', 'MUNSINP', 'MUNSIAFI', 'MUNNOME', 'MUNNOMEX', 'OBSERV',
                    'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'ANOINST', 'ANOEXT', 'SUCESSOR']:
            df1[col].replace('', '?', inplace=True)
        # Substitui uma string vazia pela string "NA" nas colunas especificadas
        df1['UFCOD_ID'].replace('', 'NA', inplace=True)
        # Substitui uma string vazia pelo float "NaN" nas colunas especificadas
        for col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']:
            df1[col].replace('', np.nan, inplace=True)
        # Converte do tipo object para float as colunas especificadas
        df1[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']] = \
        df1[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']].astype('float')
        # Coloca todas as string das colunas especificadas como UPPER CASE
        df1['MUNNOME'] = df1['MUNNOME'].apply(lambda x: x.upper())
        df1['MUNNOMEX'] = df1['MUNNOMEX'].apply(lambda x: x.upper())
        # Insere uma linha referente ao Município de Nazária/PI não constante originalmente do arquivo
        df1.loc[df1.shape[0]] = ['220672', '2206720', 'ATIVO', '?', '?', 'NAZÁRIA', 'NAZARIA', '?',
                                 'N', 'N', 'N', '22', '?', '?', '?', np.nan, np.nan, np.nan, 363.589]
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
        df1.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df1.reset_index(drop=True, inplace=True)
        # Conversão da Tabela rl_municip_regsaud para um objeto pandas DataFrame
        file_name = 'rl_municip_regsaud'
        df2 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df2.rename(index=str, columns={'CO_MUNICIP': 'ID', 'CO_REGSAUD': 'RSAUDE_ID'}, inplace=True)
        # Faz o merge de "df1" e "df2" pela coluna ID tendo por base "df1"
        df = pd.merge(df1, df2, how='left', left_on='ID', right_on='ID')
        # Converte o float NaN para a string "NA"
        df['RSAUDE_ID'].replace(np.nan, 'NA', inplace=True)
        # Reordena as colunas priorizando as "mais" relevantes
        df = df[['ID', 'MUNNOME', 'MUNNOMEX', 'MUNCODDV', 'OBSERV', 'SITUACAO', 'MUNSINP',
                 'MUNSIAFI', 'UFCOD_ID', 'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'RSAUDE_ID',
                 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA', 'ANOINST', 'ANOEXT', 'SUCESSOR']]
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?',
                               '?', '?', 'NA', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
        return df


    # Função para adequar e formatar as colunas e valores da TCC TP_GESTAO (arquivo TP_GESTAO.cnv)
    def get_TP_GESTAO_treated(self):
        # Conversão da TCC TP_GESTAO para um objeto pandas DataFrame
        file_name = 'TP_GESTAO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'GESTAO'}, inplace=True)
        # Considera da coluna GESTAO apenas a substring depois de duas letras maiúsculas e um espaço
        df['GESTAO'].replace(to_replace='^[A-Z]{2}\s{1}', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC REGRA_C (arquivo REGRA_C.cnv)
    def get_REGRA_C_treated(self):
        # Conversão da TCC REGRA_C para um objeto pandas DataFrame
        file_name = 'REGRA_C'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'REGRA'}, inplace=True)
        # Modifica um valor da coluna ID
        df.loc['0', 'ID'] = '0000'
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC TP_ESTAB (arquivo TP_ESTAB.cnv)
    def get_TP_ESTAB_treated(self):
        # Conversão da TCC TP_ESTAB para um objeto pandas DataFrame
        file_name = 'TP_ESTAB'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESTABELECIMENTO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ESFERA (arquivo ESFERA.cnv)
    def get_ESFERA_treated(self):
        # Conversão da TCC ESFERA para um objeto pandas DataFrame
        file_name = 'ESFERA'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESFERA'}, inplace=True)
        # Considera da coluna ESFERA apenas a substring depois de dois pontos
        df['ESFERA'].replace(to_replace='^\.\.', value= '', regex=True, inplace=True)
        # Modifica um valor da coluna ESFERA
        df.loc['0', 'ESFERA'] = 'ESTABELECIMENTO PRIVADO COM FINS LUCRATIVO PJ/PF'
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TB_SIGTAP (arquivo TB_SIGTAP.dbf)
    def get_TB_SIGTAP_treated(self):
        # Conversão da Tabela TB_SIGTAP para um objeto pandas DataFrame
        file_name = 'TB_SIGTAP'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'IP_COD': 'ID', 'IP_DSCR': 'PROCEDIMENTO'}, inplace=True)
        # Upload do arquivo "xlsx" que contém os PROC_SOLIC/PROC_REA presentes nos arquivos RDXXaamm (dos anos...
        # de 2008 a 2019) e não presentes no arquivo TB_SIGTAP.dbf. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'COD_PROCEDIMENTOS_OUT_TB_SIGTAP_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até...
        # formar uma "string" de tamanho = 10
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(10))
        # Adiciona a coluna DESCESTAB e respectivos valores ao objeto "dataframe"
        dataframe['PROCEDIMENTO'] = 'NAO PROVIDO NO ARQUIVO TB_SIGTAP.dbf'
        # Concatenação do objeto "dataframe" ao objeto "df"
        frames = []
        frames.append(df)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Função para adequar e formatar as colunas e valores da TCC FINANC (arquivo FINANC.cnv)
    def get_FINANC_treated(self):
        # Conversão da TCC FINANC para um objeto pandas DataFrame
        file_name = 'FINANC'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'FINANCIAMENTO'}, inplace=True)
        # Considera da coluna GESTAO apenas a substring depois de duas letras maiúsculas e um espaço
        df['FINANCIAMENTO'].replace(to_replace='^\d{2}\s{1}', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC COMPLEX (arquivo COMPLEX.cnv)
    def get_COMPLEX_treated(self):
        # Conversão da TCC COMPLEX para um objeto pandas DataFrame
        file_name = 'COMPLEX'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'COMPLEXIDADE'}, inplace=True)
        # Considera da coluna GESTAO apenas a substring depois de um dígito e um traço
        df['COMPLEXIDADE'].replace(to_replace='^\d{1}-', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC DOCORIG (arquivo DOCORIG.cnv)
    def get_DOCORIG_treated(self):
        # Conversão da TCC DOCORIG para um objeto pandas DataFrame
        file_name = 'DOCORIG'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO_SIA'}, inplace=True)
        # Considera da coluna TIPO_SIA apenas a substring depois de dois pontos
        df['TIPO_SIA'].replace(to_replace='^\.\.', value= '', regex=True, inplace=True)
        # Modifica um valor da coluna TIPO_SIA
        df.loc['5', 'TIPO_SIA'] = 'APAC - PROCEDIMENTO SECUNDÁRIO'
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela CBO (arquivo CBO.dbf)
    def get_CBO_treated(self):
        # Conversão da Tabela CBO para um objeto pandas DataFrame
        file_name = 'CBO'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" até...
        # formar uma "string" de tamanho = 6
        df['ID'] = df['ID'].apply(lambda x: x.zfill(6))
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado por representar código errado
        df = df.drop(df[df['ID']=='006540'].index)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC MOTSAIPE (arquivo MOTSAIPE.cnv)
    def get_MOTSAIPE_treated(self):
        # Conversão da TCC MOTSAIPE para um objeto pandas DataFrame
        file_name = 'MOTSAIPE'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'MOTIVO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela S_CID (arquivo S_CID.dbf).
    def get_S_CID_treated(self):
        # Conversão da Tabela S_CID para um objeto pandas DataFrame
        file_name = 'S_CID'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CD_COD': 'ID', 'CD_DESCR': 'DIAGNOSTICO'}, inplace=True)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df = df.drop(['OPC', 'CAT', 'SUBCAT', 'RESTRSEXO', 'CAMPOS_RAD', 'ESTADIO', 'REPETE_RAD'], axis=1)
        # Drop a linha inteira em que a coluna "ID" tem os valores especificados por não representarem valor útil
        df = df.drop(df[df['ID']=='0000'].index)
        df = df.drop(df[df['ID']=='9999'].index)
        ## Drop a linha inteira em que a coluna "DIAGNOSTICO" tem os valores especificados por não...
        # representarem valor útil
        df = df.drop(df[df['DIAGNOSTICO']=='CID NÇO IDENTIFICADO'].index)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df['DIAGNOSTICO'] = df['DIAGNOSTICO'].apply(lambda x: x.upper())
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC CARAT_AT (arquivo CARAT_AT.cnv)
    def get_CARAT_AT_treated(self):
        # Conversão da TCC CARAT_AT para um objeto pandas DataFrame
        file_name = 'CARAT_AT'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CARATER'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem os valores especificados por não...
        # representarem valor útil
        df = df.drop(df[df['ID']=='0'].index)
        df = df.drop(df[df['ID']=='7'].index)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC FL_IDADE (arquivo FL_IDADE.cnv)
    def get_FL_IDADE_treated(self):
        # Conversão da TCC FL_IDADE para um objeto pandas DataFrame
        file_name = 'FL_IDADE'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'COMPATIBILIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC SEXO (arquivo SEXO.cnv)
    def get_SEXO_treated(self):
        # Conversão da TCC SEXO para um objeto pandas DataFrame
        file_name = 'SEXO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'SEXO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC RACA_COR (arquivo RACA_COR.cnv)
    def get_RACA_COR_treated(self):
        # Conversão da TCC RACA_COR para um objeto pandas DataFrame
        file_name = 'RACA_COR'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'RACA'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem os valores especificados por não representarem valor útil
        df = df.drop(df[df['ID']=='00'].index)
        df = df.drop(df[df['ID']=='99'].index)
        df = df.drop(df[df['ID']=='06'].index)
        df = df.drop(df[df['ID']=='09'].index)
        df = df.drop(df[df['ID']=='1M'].index)
        # Considera da coluna ID apenas a substring depois um dígito
        df['ID'].replace(to_replace='^\d{1}', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC INDICA (arquivo INDICA.cnv)
    def get_INDICA_treated(self):
        # Conversão da TCC INDICA para um objeto pandas DataFrame
        file_name = 'INDICA'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'SITUACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC CODOCO (arquivo CODOCO.cnv)
    def get_CODOCO_treated(self):
        # Conversão da TCC CODOCO para um objeto pandas DataFrame
        file_name = 'CODOCO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'OCORRENCIA'}, inplace=True)
        # Modifica valores da coluna OCORRENCIA tal como previsto na TCC CODOCO
        df.loc['0', 'OCORRENCIA'] = '..APROVADO TOTALMENTE       (K)'
        df.loc['3', 'OCORRENCIA'] = '..ULTRAPASSOU TETO FISICO   (L)'
        df.loc['6', 'OCORRENCIA'] = '..PROCEDIMENTO SEM ORCAMENTO      (P)'
        # Substitui subtrings da coluna OCORRENCIA dependendo do valor da coluna ID
        df.loc[df.ID.str.startswith('1'), 'OCORRENCIA'] = \
        df.loc[df.ID.str.startswith('1'), 'OCORRENCIA'].replace(to_replace='^\.\.',
                                                                value= 'PRODUÇÃO TOT. APROVADA - ',
                                                                regex=True)
        df.loc[df.ID == '2L', 'OCORRENCIA'] = \
        df.loc[df.ID == '2L', 'OCORRENCIA'].replace(to_replace='^\.\.',
                                                    value= 'PRODUÇÃO PARC. APROVADA - ',
                                                    regex=True)
        df.loc[df.ID.str.startswith('3'), 'OCORRENCIA'] = \
        df.loc[df.ID.str.startswith('3'), 'OCORRENCIA'].replace(to_replace='^\.\.',
                                                                value= 'PRODUÇÃO PARC. APROVADA - ',
                                                                regex=True)
        df.loc[df.ID.str.startswith('4'), 'OCORRENCIA'] = \
        df.loc[df.ID.str.startswith('4'), 'OCORRENCIA'].replace(to_replace='^\.\.',
                                                                value= 'PRODUÇÃO NAO APROVADA - ',
                                                                regex=True)
        df.loc[df.ID == '5O', 'OCORRENCIA'] = \
        df.loc[df.ID == '5O', 'OCORRENCIA'].replace(to_replace='^\.\.',
                                                    value= 'PRODUÇÃO NAO APROVADA - ',
                                                    regex=True)
        # Desconsidera da coluna OCORRENCIA valores que terminam com uma substring da forma "(\.)"
        df['OCORRENCIA'].replace(to_replace='\(.\)$', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC etnia (arquivo etnia.cnv)
    def get_etnia_treated(self):
        # Conversão da TCC etnia para um objeto pandas DataFrame
        file_name = 'etnia'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'INDIGENA'}, inplace=True)
        # Preenche os valores da coluna ID com zeros a esquerda até formar quatro digitos
        df['ID'] = df['ID'].apply(lambda x: x.zfill(4))
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela INE_EQUIPE_BR (arquivo INE_EQUIPE_BR.dbf)
    def get_INE_EQUIPE_BR_treated(self):
        # Conversão da Tabela INE_EQUIPE_BR para um objeto pandas DataFrame
        file_name = 'INE_EQUIPE_BR'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CO_INE': 'ID', 'NO_REF': 'EQUIPE'}, inplace=True)
        # Considera da coluna EQUIPE apenas a substring depois de dez dígitos e um espaço
        df['EQUIPE'].replace(to_replace='^\d{10} ', value= '', regex=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC natjur (arquivo natjur.cnv)
    def get_natjur_treated(self):
        # Conversão da TCC natjur para um objeto pandas DataFrame
        file_name = 'natjur'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nada
        df = df.drop(df[df['ID']=='0'].index)
        # Reset o index devido à eliminação de linha efetuada no passo anterior
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TABUF (arquivo TABUF.dbf)
    def get_TABUF_treated(self):
        # Conversão da Tabela TABUF para um objeto pandas DataFrame
        file_name = 'TABUF'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'ESTADO'}, inplace=True)
        # Reordena as colunas
        df = df[['ID', 'ESTADO', 'SIGLA_UF']]
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela RSAUDE (do IBGE)
    def get_RSAUDE_treated(self):
        # Conversão da Tabela RSAUDE (em formato "xlsx") para um objeto pandas DataFrame
        df = pd.read_excel(self.path + 'RSAUDE' + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'REGIAO'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TB_GRUPO (arquivo TB_GRUPO.dbf)
    def get_TB_GRUPO_treated(self):
        # Conversão da Tabela TB_GRUPO para um objeto pandas DataFrame
        file_name = 'TB_GRUPO'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CO_GRUPO': 'ID', 'NO_GRUPO': 'GRUPO'}, inplace=True)
        # Remove coluna indesejáveL do objeto pandas DataFrame
        df = df.drop(['DT_COMPET'], axis=1)
        # Torna UPPERCASE os valores da coluna GRUPO
        df['GRUPO'] = df['GRUPO'].str.upper()
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TB_SUBGR (arquivo TB_SUBGR.dbf)
    def get_TB_SUBGR_treated(self):
        # Conversão da Tabela TB_SUBGR para um objeto pandas DataFrame
        file_name = 'TB_SUBGR'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CO_SUB_GRU': 'ID', 'NO_SUB_GRU': 'SUBGRUPO'}, inplace=True)
        # Remove coluna indesejáveL do objeto pandas DataFrame
        df = df.drop(['DT_COMPET'], axis=1)
        # Torna UPPERCASE os valores da coluna SUBGRUPO
        df['SUBGRUPO'] = df['SUBGRUPO'].str.upper()
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TB_FORMA (arquivo TB_FORMA.dbf)
    def get_TB_FORMA_treated(self):
        # Conversão da Tabela TB_FORMA para um objeto pandas DataFrame
        file_name = 'TB_FORMA'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CO_FORMA': 'ID', 'NO_FORMA': 'FORMA'}, inplace=True)
        # Remove coluna indesejáveL do objeto pandas DataFrame
        df = df.drop(['DT_COMPET'], axis=1)
        # Torna UPPERCASE os valores da coluna FORMA
        df['FORMA'] = df['FORMA'].str.upper()
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Ajustar "the_path" para a localização dos arquivos "xlsx"
    the_path = os.getcwd()[:-len('\\transform')] + '\\files\\SIA\\'

    instancia = DataSiaAuxiliary(the_path)

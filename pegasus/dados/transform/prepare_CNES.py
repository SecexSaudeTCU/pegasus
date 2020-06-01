############################################################################################################################
#  CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES #
############################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from transform.extract.download_CNES import download_CNESXXaamm, download_table_dbf, download_table_cnv

"""
Módulo de limpeza/tratamento de dados do CNES.

"""

# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Classe de dados principais do CNES
class DataCnesMain:

    # Construtor
    def __init__(self, base, state, year, month):
        self.base = base
        self.state = state
        self.year = year
        self.month = month


    # Método para ler como um objeto pandas DataFrame um arquivo principal de dados do CNES e adequar e formatar suas colunas...
    # e valores
    def get_CNESXXaamm_treated(self):
        # Lê o arquivo "dbc" ou "parquet", se já tiver sido baixado, como um objeto pandas DataFrame
        dataframe = download_CNESXXaamm(self.base, self.state, self.year, self.month)
        print(f'O número de linhas do arquivo {self.base}{self.state}{self.year}{self.month} é {dataframe.shape[0]}.')

        ###################################################################################################################
        # CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
        ###################################################################################################################
        if self.base == 'ST':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela stbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'COD_CEP', 'CPF_CNPJ', 'PF_PJ', 'NIV_DEP', 'CNPJ_MAN', 'COD_IR',
                                      'VINC_SUS', 'TPGESTAO', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL',
                                      'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'CO_BANCO', 'CO_AGENC', 'C_CORREN',
                                      'ALVARA', 'DT_EXPED', 'ORGEXPED', 'AV_ACRED', 'CLASAVAL', 'DT_ACRED', 'AV_PNASS',
                                      'DT_PNASS', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E', 'GESPRG2M', 'GESPRG4E', 'GESPRG4M',
                                      'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 'GESPRG5E', 'GESPRG5M', 'GESPRG6E', 'GESPRG6M',
                                      'NIVATE_H', 'QTLEITP1', 'QTLEITP2', 'QTLEITP3', 'LEITHOSP', 'QTINST01', 'QTINST02',
                                      'QTINST03', 'QTINST04', 'QTINST05', 'QTINST06', 'QTINST07', 'QTINST08', 'QTINST09',
                                      'QTINST10', 'QTINST11', 'QTINST12', 'QTINST13', 'QTINST14', 'URGEMERG', 'QTINST15',
                                      'QTINST16', 'QTINST17', 'QTINST18', 'QTINST19', 'QTINST20', 'QTINST21', 'QTINST22',
                                      'QTINST23', 'QTINST24', 'QTINST25', 'QTINST26', 'QTINST27', 'QTINST28', 'QTINST29',
                                      'QTINST30', 'ATENDAMB', 'QTINST31', 'QTINST32', 'QTINST33', 'CENTRCIR', 'QTINST34',
                                      'QTINST35', 'QTINST36', 'QTINST37', 'CENTROBS', 'QTLEIT05', 'QTLEIT06', 'QTLEIT07',
                                      'QTLEIT09', 'QTLEIT19', 'QTLEIT20', 'QTLEIT21', 'QTLEIT22', 'QTLEIT23', 'QTLEIT32',
                                      'QTLEIT34', 'QTLEIT38', 'QTLEIT39', 'QTLEIT40', 'CENTRNEO', 'ATENDHOS', 'SERAP01P',
                                      'SERAP01T', 'SERAP02P', 'SERAP02T', 'SERAP03P', 'SERAP03T', 'SERAP04P', 'SERAP04T',
                                      'SERAP05P', 'SERAP05T', 'SERAP06P', 'SERAP06T', 'SERAP07P', 'SERAP07T', 'SERAP08P',
                                      'SERAP08T', 'SERAP09P', 'SERAP09T', 'SERAP10P', 'SERAP10T', 'SERAP11P', 'SERAP11T',
                                      'SERAPOIO', 'RES_BIOL', 'RES_QUIM', 'RES_RADI', 'RES_COMU', 'COLETRES', 'COMISS01',
                                      'COMISS02', 'COMISS03', 'COMISS04', 'COMISS05', 'COMISS06', 'COMISS07', 'COMISS08',
                                      'COMISS09', 'COMISS10', 'COMISS11', 'COMISS12', 'COMISSAO', 'AP01CV01', 'AP01CV02',
                                      'AP01CV05', 'AP01CV06', 'AP01CV03', 'AP01CV04', 'AP02CV01', 'AP02CV02', 'AP02CV05',
                                      'AP02CV06', 'AP02CV03', 'AP02CV04', 'AP03CV01', 'AP03CV02', 'AP03CV05', 'AP03CV06',
                                      'AP03CV03', 'AP03CV04', 'AP04CV01', 'AP04CV02', 'AP04CV05', 'AP04CV06', 'AP04CV03',
                                      'AP04CV04', 'AP05CV01', 'AP05CV02', 'AP05CV05', 'AP05CV06', 'AP05CV03', 'AP05CV04',
                                      'AP06CV01', 'AP06CV02', 'AP06CV05', 'AP06CV06', 'AP06CV03', 'AP06CV04', 'AP07CV01',
                                      'AP07CV02', 'AP07CV05', 'AP07CV06', 'AP07CV03', 'AP07CV04', 'ATEND_PR', 'NAT_JUR'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na...
            # variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
            # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            df['C_CORREN'] = df['C_CORREN'].apply(lambda x: str(tryconvert(x, '', int)))

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['NIV_DEP'].replace('5', '', inplace=True)

            df['COD_IR'].replace('00', '', inplace=True)
            df['COD_IR'].replace('IR', '19', inplace=True)

            df['TPGESTAO'].replace('S', 'Z', inplace=True)

            for col in np.array(['ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA',
                                 'CLIENTEL', 'TURNO_AT', 'TP_PREST']):
                df[col].replace('99', '', inplace=True)

            df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

            df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

            df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if len(x) == 8 else '')
            df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')
            df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if 1 <= tryconvert(x[6:8], 0, int) <= 31 else '')

            df['ORGEXPED'].replace('9', '', inplace=True)

            for col in np.array(['AV_ACRED', 'AV_PNASS']):
                df[col].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)
                df[col].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                        # string "0" do domínio binário

            df['CLASAVAL'].replace(['N', 'A'], '', inplace=True)

            df['NAT_JUR'].replace('1333', '1000', inplace=True)
            df['NAT_JUR'].replace('2100', '2000', inplace=True)
            df['NAT_JUR'].replace('3301', '3000', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'PF_PJ', 'NIV_DEP', 'COD_IR', 'TPGESTAO', 'ESFERA_A',
                                 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT',
                                 'NIV_HIER', 'TP_PREST', 'ORGEXPED', 'CLASAVAL', 'NAT_JUR']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['COD_CEP', 'CPF_CNPJ', 'CNPJ_MAN', 'CO_BANCO', 'CO_AGENC', 'C_CORREN', 'ALVARA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: datetime.strptime(x, '%Y%m%d').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            for col in np.array(['DT_ACRED', 'DT_PNASS']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
            # data futura "2099-01-01"
            for col in np.array(['DT_EXPED', 'DT_ACRED', 'DT_PNASS']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['VINC_SUS', 'AV_ACRED', 'AV_PNASS', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E', 'GESPRG2M',
                                 'GESPRG4E', 'GESPRG4M', 'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 'GESPRG5E', 'GESPRG5M',
                                 'GESPRG6E', 'GESPRG6M', 'NIVATE_H', 'LEITHOSP', 'URGEMERG', 'ATENDAMB', 'CENTRCIR',
                                 'CENTROBS', 'CENTRNEO', 'ATENDHOS', 'SERAP01P', 'SERAP01T', 'SERAP02P', 'SERAP02T',
                                 'SERAP03P', 'SERAP03T', 'SERAP04P', 'SERAP04T', 'SERAP05P', 'SERAP05T', 'SERAP06P',
                                 'SERAP06T', 'SERAP07P', 'SERAP07T', 'SERAP08P', 'SERAP08T', 'SERAP09P', 'SERAP09T',
                                 'SERAP10P', 'SERAP10T', 'SERAP11P', 'SERAP11T', 'SERAPOIO', 'RES_BIOL', 'RES_QUIM',
                                 'RES_RADI', 'RES_COMU', 'COLETRES', 'COMISS01', 'COMISS02', 'COMISS03', 'COMISS04',
                                 'COMISS05', 'COMISS06', 'COMISS07', 'COMISS08', 'COMISS09', 'COMISS10', 'COMISS11',
                                 'COMISS12', 'COMISSAO', 'AP01CV01', 'AP01CV02', 'AP01CV05', 'AP01CV06', 'AP01CV03',
                                 'AP01CV04', 'AP02CV01', 'AP02CV02', 'AP02CV05', 'AP02CV06', 'AP02CV03', 'AP02CV04',
                                 'AP03CV01', 'AP03CV02', 'AP03CV05', 'AP03CV06', 'AP03CV03', 'AP03CV04', 'AP04CV01',
                                 'AP04CV02', 'AP04CV05', 'AP04CV06', 'AP04CV03', 'AP04CV04', 'AP05CV01', 'AP05CV02',
                                 'AP05CV05', 'AP05CV06', 'AP05CV03', 'AP05CV04', 'AP06CV01', 'AP06CV02', 'AP06CV05',
                                 'AP06CV06', 'AP06CV03', 'AP06CV04', 'AP07CV01', 'AP07CV02', 'AP07CV05', 'AP07CV06',
                                 'AP07CV03', 'AP07CV04', 'ATEND_PR']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['QTLEITP1', 'QTLEITP2', 'QTLEITP3', 'QTINST01', 'QTINST02', 'QTINST03', 'QTINST04',
                                 'QTINST05', 'QTINST06', 'QTINST07', 'QTINST08', 'QTINST09', 'QTINST10', 'QTINST11',
                                 'QTINST12', 'QTINST13', 'QTINST14', 'QTINST15', 'QTINST16', 'QTINST17', 'QTINST18',
                                 'QTINST19', 'QTINST20', 'QTINST21', 'QTINST22', 'QTINST23', 'QTINST24', 'QTINST25',
                                 'QTINST26', 'QTINST27', 'QTINST28', 'QTINST29', 'QTINST30', 'QTINST31', 'QTINST32',
                                 'QTINST33', 'QTINST34', 'QTINST35', 'QTINST36', 'QTINST37', 'QTLEIT05', 'QTLEIT06',
                                 'QTLEIT07', 'QTLEIT09', 'QTLEIT19', 'QTLEIT20', 'QTLEIT21', 'QTLEIT22', 'QTLEIT23',
                                 'QTLEIT32', 'QTLEIT34', 'QTLEIT38', 'QTLEIT39', 'QTLEIT40']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'PF_PJ': 'PFPJ_ID', 'NIV_DEP': 'NIVDEP_ID',
                                          'COD_IR': 'CODIR_ID', 'TPGESTAO': 'TPGESTAO_ID',
                                          'ESFERA_A': 'ESFERAA_ID', 'RETENCAO': 'RETENCAO_ID',
                                          'ATIVIDAD': 'ATIVIDAD_ID', 'NATUREZA': 'NATUREZA_ID',
                                          'CLIENTEL': 'CLIENTEL_ID', 'TP_UNID': 'TPUNID_ID',
                                          'TURNO_AT': 'TURNOAT_ID', 'NIV_HIER': 'NIVHIER_ID',
                                          'TP_PREST': 'TPPREST_ID', 'ORGEXPED': 'ORGEXPED_ID',
                                          'CLASAVAL': 'CLASAVAL_ID', 'NAT_JUR': 'NATJUR_ID'}, inplace=True)

            print(f'Tratou o arquivo ST{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC CNES_DC #
        ###################################################################################################################
        elif self.base == 'DC':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dcbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'S_HBSAGP', 'S_HBSAGN', 'S_DPI', 'S_DPAC', 'S_REAGP',
                                      'S_REAGN', 'S_REHCV', 'MAQ_PROP', 'MAQ_OUTR', 'F_AREIA', 'F_CARVAO',
                                      'ABRANDAD', 'DEIONIZA', 'OSMOSE_R', 'OUT_TRAT', 'CNS_NEFR', 'DIALISE',
                                      'SIMUL_RD', 'PLANJ_RD', 'ARMAZ_FT', 'CONF_MAS', 'SALA_MOL', 'BLOCOPER',
                                      'S_ARMAZE', 'S_PREPAR', 'S_QCDURA', 'S_QLDURA', 'S_CPFLUX', 'S_SIMULA',
                                      'S_ACELL6', 'S_ALSEME', 'S_ALCOME', 'ORTV1050', 'ORV50150', 'OV150500',
                                      'UN_COBAL', 'EQBRBAIX', 'EQBRMEDI', 'EQBRALTA', 'EQ_MAREA', 'EQ_MINDI',
                                      'EQSISPLN', 'EQDOSCLI', 'EQFONSEL', 'CNS_ADM', 'CNS_OPED', 'CNS_CONC',
                                      'CNS_OCLIN', 'CNS_MRAD', 'CNS_FNUC', 'QUIMRADI', 'S_RECEPC', 'S_TRIHMT',
                                      'S_TRICLI', 'S_COLETA', 'S_AFERES', 'S_PREEST', 'S_PROCES', 'S_ESTOQU',
                                      'S_DISTRI', 'S_SOROLO', 'S_IMUNOH', 'S_PRETRA', 'S_HEMOST', 'S_CONTRQ',
                                      'S_BIOMOL', 'S_IMUNFE', 'S_TRANSF', 'S_SGDOAD', 'QT_CADRE', 'QT_CENRE',
                                      'QT_REFSA', 'QT_CONRA', 'QT_EXTPL', 'QT_FRE18', 'QT_FRE30', 'QT_AGIPL',
                                      'QT_SELAD', 'QT_IRRHE', 'QT_AGLTN', 'QT_MAQAF', 'QT_REFRE', 'QT_REFAS',
                                      'QT_CAPFL', 'CNS_HMTR', 'CNS_HMTL', 'CNS_CRES', 'CNS_RTEC', 'HEMOTERA'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na...
            # variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
            # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['CNS_NEFR', 'CNS_ADM', 'CNS_OPED', 'CNS_CONC', 'CNS_OCLIN', 'CNS_MRAD',
                                 'CNS_FNUC', 'CNS_HMTR', 'CNS_HMTL', 'CNS_CRES', 'CNS_RTEC']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['F_AREIA', 'F_CARVAO', 'ABRANDAD', 'DEIONIZA', 'OSMOSE_R',
                                 'OUT_TRAT', 'DIALISE', 'QUIMRADI', 'HEMOTERA']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['S_HBSAGP', 'S_HBSAGN', 'S_DPI', 'S_DPAC', 'S_REAGP', 'S_REAGN', 'S_REHCV',
                                 'MAQ_PROP', 'MAQ_OUTR', 'SIMUL_RD', 'PLANJ_RD', 'ARMAZ_FT', 'CONF_MAS',
                                 'SALA_MOL', 'BLOCOPER', 'S_ARMAZE', 'S_PREPAR', 'S_QCDURA', 'S_QLDURA',
                                 'S_CPFLUX', 'S_SIMULA', 'S_ACELL6', 'S_ALSEME', 'S_ALCOME', 'ORTV1050',
                                 'ORV50150', 'OV150500', 'UN_COBAL', 'EQBRBAIX', 'EQBRMEDI', 'EQBRALTA',
                                 'EQ_MAREA', 'EQ_MINDI', 'EQSISPLN', 'EQDOSCLI', 'EQFONSEL', 'S_RECEPC',
                                 'S_TRIHMT', 'S_TRICLI', 'S_COLETA', 'S_AFERES', 'S_PREEST', 'S_PROCES',
                                 'S_ESTOQU', 'S_DISTRI', 'S_SOROLO', 'S_IMUNOH', 'S_PRETRA', 'S_HEMOST',
                                 'S_CONTRQ', 'S_BIOMOL', 'S_IMUNFE', 'S_TRANSF', 'S_SGDOAD', 'QT_CADRE',
                                 'QT_CENRE', 'QT_REFSA', 'QT_CONRA', 'QT_EXTPL', 'QT_FRE18', 'QT_FRE30',
                                 'QT_AGIPL', 'QT_SELAD', 'QT_IRRHE', 'QT_AGLTN', 'QT_MAQAF', 'QT_REFRE',
                                 'QT_REFAS', 'QT_CAPFL']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID'}, inplace=True)

            print(f'Tratou o arquivo DC{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
        ###################################################################################################################
        elif self.base == 'PF':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela pfbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'CBO', 'CBOUNICO', 'NOMEPROF', 'CNS_PROF',
                                      'CONSELHO', 'REGISTRO', 'VINCULAC', 'VINCUL_C', 'VINCUL_A',
                                      'VINCUL_N', 'PROF_SUS', 'PROFNSUS', 'HORAOUTR', 'HORAHOSP', 'HORA_AMB'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na...
            # variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
            # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['CBO', 'CBOUNICO', 'VINCULAC']):
                df[col] = df[col].apply(lambda x: x.zfill(6))
                df[col] = df[col].apply(str.strip)
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')

            df['CONSELHO'] = df['CONSELHO'].apply(lambda x: x.zfill(2))
            df['CONSELHO'] = df['CONSELHO'].apply(str.strip)
            df['CONSELHO'] = df['CONSELHO'].apply(lambda x: x if len(x) == 2 else '')

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            for col in np.array(['CBO', 'CBOUNICO', 'VINCULAC']):
                df[col].replace('000000', '', inplace=True)

            df['CONSELHO'].replace(['00', '01', '02', '05', '06', '08', '09', '11',
                                    '13', '14', '16', '27', '31', '98', '99'], '', inplace=True)

            df['VINCULAC'].replace(['010603', '040101'], '', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'CBO', 'CBOUNICO', 'CONSELHO', 'VINCULAC']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['NOMEPROF', 'CNS_PROF', 'REGISTRO']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['VINCUL_C', 'VINCUL_A', 'VINCUL_N', 'PROF_SUS', 'PROFNSUS']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['HORAOUTR', 'HORAHOSP', 'HORA_AMB']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'CBO': 'CBO_ID', 'CBOUNICO': 'CBOUNICO_ID',
                                          'CONSELHO': 'CONSELHO_ID', 'VINCULAC': 'VINCULAC_ID'}, inplace=True)

            print(f'Tratou o arquivo PF{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT #
        ###################################################################################################################
        elif self.base == 'LT':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela ltbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TP_LEITO', 'CODLEITO',
                                      'QT_EXIST', 'QT_CONTR', 'QT_SUS'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na...
            # variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
            # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            df['CODLEITO'] = df['CODLEITO'].apply(lambda x: str(tryconvert(x, '', int)))

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'TP_LEITO', 'CODLEITO']):
                df[col].replace('', 'NA', inplace=True)

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['QT_EXIST', 'QT_CONTR', 'QT_SUS']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'TP_LEITO': 'TPLEITO_ID', 'CODLEITO': 'CODLEITO_ID'}, inplace=True)

            print(f'Tratou o arquivo LT{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ #
        ###################################################################################################################
        elif self.base == 'EQ':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela eqbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TIPEQUIP', 'CODEQUIP',
                                      'QT_EXIST', 'QT_USO', 'IND_SUS', 'ND_NSUS'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na...
            # variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
            # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            df['CODEQUIP'] = df['CODEQUIP'].apply(lambda x: str(tryconvert(x, '', int)))

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['CODEQUIP'].replace(['19', '20', '24', '25', '26', '27', '28', '29', '30',
                                    '43', '65', '66', '67', '68', '69', '70', '79'], '', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'TIPEQUIP', 'CODEQUIP']):
                df[col].replace('', 'NA', inplace=True)

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['IND_SUS', 'ND_NSUS']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['QT_EXIST', 'QT_USO']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'TIPEQUIP': 'TIPEQUIP_ID', 'CODEQUIP': 'CODEQUIP_ID'}, inplace=True)

            print(f'Tratou o arquivo EQ{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR #
        ###################################################################################################################
        elif self.base == 'SR':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela srbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'SRVUNICO', 'TPGESTAO',
                                      'PF_PJ', 'CPF_CNPJ', 'NIV_DEP', 'ESFERA_A', 'ATIVIDAD', 'RETENCAO',
                                      'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TERCEIRO',
                                      'CNPJ_MAN', 'CARACTER', 'AMB_NSUS', 'AMB_SUS', 'HOSP_NSUS', 'HOSP_SUS',
                                      'CONTSRVU', 'CNESTERC', 'NAT_JUR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Cria uma lista que é a justaposição dos códigos de três dígitos das colunas SERV_ESP e CLASS_SR
            _col = (df['SERV_ESP'].map(str) + df['CLASS_SR']).to_list()
            # Remove a coluna CLASS_SR do objeto pandas DataFrame "df"
            df = df.drop(['CLASS_SR'], axis=1)
            # Insere coluna de mesmo nome e na mesma posição da coluna removida em "df" com os valores do objeto list "_col"
            df.insert(3, 'CLASS_SR', _col)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas
            for col in np.array(['SERV_ESP', 'SRVUNICO']):
                df[col] = df[col].apply(lambda x: x.zfill(3))
                df[col] = df[col].apply(str.strip)
                df[col] = df[col].apply(lambda x: x if len(x) == 3 else '')

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL',
                                 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            for col in np.array(['SERV_ESP', 'SRVUNICO']):
                df[col].replace(['000', '023', '026', '137', '138'], '', inplace=True)

            df['CLASS_SR'].replace(regex=['^\d{3}0{3}$', '^\d{3}9{3}$','^13[78]\d{3}$'], value='', inplace=True)
            df['CLASS_SR'].replace(['006053', '121005', '130002', '026109',
                                    '500001', '500002', '513003'], '', inplace=True)

            df['TPGESTAO'].replace('S', 'Z', inplace=True)

            df['NIV_DEP'].replace('5', '', inplace=True)

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'RETENCAO', 'NATUREZA', 'CLIENTEL', 'TURNO_AT']):
                df[col].replace('99', '', inplace=True)

            df['ATIVIDAD'].replace('p4', '', inplace=True)

            df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

            df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto
                                                           # string "0" do domínio binário

            df['CARACTER'].replace('9', '', inplace=True)

            df['NAT_JUR'].replace('1333', '1000', inplace=True)
            df['NAT_JUR'].replace('2100', '2000', inplace=True)
            df['NAT_JUR'].replace('3301', '3000', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'SRVUNICO', 'TPGESTAO',
                                 'PF_PJ', 'NIV_DEP', 'ESFERA_A', 'ATIVIDAD', 'RETENCAO', 'NATUREZA',
                                 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'CARACTER', 'NAT_JUR']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['CPF_CNPJ', 'CNPJ_MAN', 'CNESTERC']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO', 'AMB_NSUS', 'AMB_SUS', 'HOSP_NSUS', 'HOSP_SUS', 'CONTSRVU']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'SERV_ESP': 'SERVESP_ID', 'CLASS_SR': 'CLASSSR_ID',
                                          'SRVUNICO': 'SRVUNICO_ID', 'TPGESTAO': 'TPGESTAO_ID',
                                          'PF_PJ': 'PFPJ_ID', 'NIV_DEP': 'NIVDEP_ID',
                                          'ESFERA_A': 'ESFERAA_ID', 'ATIVIDAD': 'ATIVIDAD_ID',
                                          'RETENCAO': 'RETENCAO_ID', 'NATUREZA': 'NATUREZA_ID',
                                          'CLIENTEL':'CLIENTEL_ID', 'TP_UNID': 'TPUNID_ID',
                                          'TURNO_AT': 'TURNOAT_ID', 'NIV_HIER': 'NIVHIER_ID',
                                          'CARACTER': 'CARACTER_ID', 'NAT_JUR': 'NATJUR_ID'}, inplace=True)

            print(f'Tratou o arquivo SR{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
        ###################################################################################################################
        elif self.base == 'EP':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela epbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'IDEQUIPE', 'TIPO_EQP', 'NOME_EQP', 'ID_AREA',
                                      'NOMEAREA', 'ID_SEGM', 'DESCSEGM', 'TIPOSEGM', 'DT_ATIVA', 'DT_DESAT',
                                      'QUILOMBO', 'ASSENTAD', 'POPGERAL', 'ESCOLA', 'INDIGENA', 'PRONASCI',
                                      'MOTDESAT', 'TP_DESAT'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['MOTDESAT', 'TP_DESAT']):
                df[col] = df[col].apply(lambda x: str(tryconvert(x, '', int)))

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TIPO_EQP'].replace(['58', '59', '60'], '', inplace=True)

            df['ID_AREA'].replace(regex=['^1[1-7][09]{8}$', '^2[1-9][09]{8}$','^3[1235][09]{8}$',
                                  '^4[1-3][09]{8}$', '^5[0-3][09]{8}$'], value='', inplace=True)

            df['ID_SEGM'].replace(regex=['^1[1-7][09]{6}$', '^2[1-9][09]{6}$','^3[1235][09]{6}$',
                                         '^4[1-3][09]{6}$', '^5[0-3][09]{6}$'], value='', inplace=True)

            df['TIPOSEGM'].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)

            df['DT_DESAT'].replace('900001', '', inplace=True)

            df['MOTDESAT'].replace(regex='[1-9][0-9]', value='', inplace=True)

            df['TP_DESAT'].replace(regex='[3-9]', value='', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'IDEQUIPE', 'TIPO_EQP', 'ID_AREA',
                                 'ID_SEGM', 'TIPOSEGM', 'MOTDESAT', 'TP_DESAT']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['NOME_EQP', 'NOMEAREA', 'DESCSEGM']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['DT_ATIVA', 'DT_DESAT']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
            # data futura "2099-01-01"
            for col in np.array(['DT_ATIVA', 'DT_DESAT']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['QUILOMBO', 'ASSENTAD', 'POPGERAL', 'ESCOLA', 'INDIGENA', 'PRONASCI']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'IDEQUIPE': 'IDEQUIPE_ID', 'TIPO_EQP': 'TIPOEQP_ID',
                                          'ID_AREA': 'IDAREA_ID', 'ID_SEGM': 'IDSEGM_ID',
                                          'TIPOSEGM': 'TIPOSEGM_ID', 'MOTDESAT': 'MOTDESAT_ID',
                                          'TP_DESAT': 'TPDESAT_ID'}, inplace=True)

            print(f'Tratou o arquivo EP{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB #
        ###################################################################################################################
        elif self.base == 'HB':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela hbbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TERCEIRO', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR', 'NULEITOS'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x.zfill(4))
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(str.strip)
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x if len(x) == 4 else '')

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                           # string "0" do domínio binário

            df['SGRUPHAB'].replace(['0637', '0914', '2430', '9101'], '', inplace=True)

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'SGRUPHAB']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de...
            # quantidades ou para o valor None caso a coluna esteja com a string vazia
            for col in np.array(['NULEITOS']):
                df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'SGRUPHAB': 'SGRUPHAB_ID'}, inplace=True)

            print(f'Tratou o arquivo HB{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df


        ###################################################################################################################
        # CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC #
        ###################################################################################################################
        elif self.base == 'RC':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela rcbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TERCEIRO', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x.zfill(4))
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(str.strip)
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x if len(x) == 4 else '')

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                           # string "0" do domínio binário

            df['SGRUPHAB'].replace(['0914', '2430', '9101'], '', inplace=True)

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'SGRUPHAB']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'SGRUPHAB': 'SGRUPHAB_ID'}, inplace=True)

            print(f'Tratou o arquivo RC{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM #
        ###################################################################################################################
        elif self.base == 'GM':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela gmbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TERCEIRO', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x.zfill(4))
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(str.strip)
            df['SGRUPHAB'] = df['SGRUPHAB'].apply(lambda x: x if len(x) == 4 else '')

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto
                                                           # string "0" do domínio binário

            #df['SGRUPHAB'].replace(['0914', '2430', '9101'], '', inplace=True)

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'SGRUPHAB']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'SGRUPHAB': 'SGRUPHAB_ID'}, inplace=True)

            print(f'Tratou o arquivo GM{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE #
        ###################################################################################################################
        elif self.base == 'EE':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela eebr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'PF_PJ', 'CPF_CNPJ', 'NIV_DEP',
                                      'CNPJ_MAN', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA',
                                      'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TERCEIRO',
                                      'COD_CEP', 'VINC_SUS', 'TP_PREST', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR', 'NAT_JUR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TPGESTAO'].replace('S', 'Z', inplace=True)

            df['NIV_DEP'].replace('5', '', inplace=True)

            for col in np.array(['ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TURNO_AT', 'TP_PREST']):
                df[col].replace('99', '', inplace=True)

            df['ATIVIDAD'].replace('p4', '', inplace=True)

            df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

            df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto
                                                           # string "0" do domínio binário

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            df['NAT_JUR'].replace('1333', '1000', inplace=True)
            df['NAT_JUR'].replace('2100', '2000', inplace=True)
            df['NAT_JUR'].replace('3301', '3000', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'NIV_DEP', 'ESFERA_A',
                                 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID',
                                 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'SGRUPHAB', 'NAT_JUR']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['CPF_CNPJ', 'CNPJ_MAN', 'COD_CEP', 'PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO', 'VINC_SUS']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'TPGESTAO': 'TPGESTAO_ID', 'PF_PJ': 'PFPJ_ID',
                                          'NIV_DEP': 'NIVDEP_ID', 'ESFERA_A': 'ESFERAA_ID',
                                          'RETENCAO': 'RETENCAO_ID', 'ATIVIDAD': 'ATIVIDAD_ID',
                                          'NATUREZA': 'NATUREZA_ID', 'CLIENTEL': 'CLIENTEL_ID',
                                          'TP_UNID': 'TPUNID_ID', 'TURNO_AT': 'TURNOAT_ID',
                                          'NIV_HIER': 'NIVHIER_ID', 'TP_PREST': 'TPPREST_ID',
                                          'SGRUPHAB': 'SGRUPHAB_ID', 'NAT_JUR': 'NATJUR_ID'}, inplace=True)

            print(f'Tratou o arquivo EE{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df


        ###################################################################################################################
        # CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF #
        ###################################################################################################################
        elif self.base == 'EF':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela efbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'PF_PJ', 'CPF_CNPJ', 'NIV_DEP',
                                      'CNPJ_MAN', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA',
                                      'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TERCEIRO',
                                      'COD_CEP', 'VINC_SUS', 'TP_PREST', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR', 'NAT_JUR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TPGESTAO'].replace('S', 'Z', inplace=True)

            df['NIV_DEP'].replace('5', '', inplace=True)

            for col in np.array(['ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TURNO_AT', 'TP_PREST']):
                df[col].replace('99', '', inplace=True)

            df['ATIVIDAD'].replace('p4', '', inplace=True)

            df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

            df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto
                                                           # string "0" do domínio binário

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            df['NAT_JUR'].replace('1333', '1000', inplace=True)
            df['NAT_JUR'].replace('2100', '2000', inplace=True)
            df['NAT_JUR'].replace('3301', '3000', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'NIV_DEP', 'ESFERA_A',
                                 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID',
                                 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'SGRUPHAB', 'NAT_JUR']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['CPF_CNPJ', 'CNPJ_MAN', 'COD_CEP', 'PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
            # data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO', 'VINC_SUS']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'TPGESTAO': 'TPGESTAO_ID', 'PF_PJ': 'PFPJ_ID',
                                          'NIV_DEP': 'NIVDEP_ID', 'ESFERA_A': 'ESFERAA_ID',
                                          'RETENCAO': 'RETENCAO_ID', 'ATIVIDAD': 'ATIVIDAD_ID',
                                          'NATUREZA': 'NATUREZA_ID', 'CLIENTEL': 'CLIENTEL_ID',
                                          'TP_UNID': 'TPUNID_ID', 'TURNO_AT': 'TURNOAT_ID',
                                          'NIV_HIER': 'NIVHIER_ID', 'TP_PREST': 'TPPREST_ID',
                                          'SGRUPHAB': 'SGRUPHAB_ID', 'NAT_JUR': 'NATJUR_ID'}, inplace=True)

            print(f'Tratou o arquivo EF{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df

        ###################################################################################################################
        # CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN CNES_IN #
        ###################################################################################################################
        elif self.base == 'IN':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela inbr
            lista_columns = np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'PF_PJ', 'CPF_CNPJ', 'NIV_DEP',
                                      'CNPJ_MAN', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA',
                                      'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TERCEIRO',
                                      'COD_CEP', 'VINC_SUS', 'TP_PREST', 'SGRUPHAB', 'CMPT_INI',
                                      'CMPT_FIM', 'DTPORTAR', 'PORTARIA', 'MAPORTAR', 'NAT_JUR'])

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
            # presentes navariável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
            # código do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
            if len(df.loc[0, 'CODUFMUN']) == 7:
                df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

            # Simplifica/corrige a apresentação dos dados das colunas especificadas

            for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
                for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                    df[col].replace(i, str(int(i)), inplace=True)

            # Atualiza/corrige os labels das colunas especificadas

            df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
            df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454',
                                    '500627', '510445', '999999'], '', inplace=True)
            df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080',
                                    '530090', '530100', '530110',  '530120', '530130', '530135', '530140',
                                    '530150', '530160', '530170', '530180'] \
                                     + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['TPGESTAO'].replace('S', 'Z', inplace=True)

            df['NIV_DEP'].replace('5', '', inplace=True)

            for col in np.array(['ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TURNO_AT', 'TP_PREST']):
                df[col].replace('99', '', inplace=True)

            df['ATIVIDAD'].replace('p4', '', inplace=True)

            df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

            df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

            df['TERCEIRO'].replace('9', '', inplace=True)
            df['TERCEIRO'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto
                                                           # string "0" do domínio binário

            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
                df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')

            df['NAT_JUR'].replace('1333', '1000', inplace=True)
            df['NAT_JUR'].replace('2100', '2000', inplace=True)
            df['NAT_JUR'].replace('3301', '3000', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in np.array(['CNES', 'CODUFMUN', 'TPGESTAO', 'NIV_DEP', 'ESFERA_A',
                                 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL',
                                 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'NAT_JUR']):
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in np.array(['CPF_CNPJ', 'CNPJ_MAN', 'COD_CEP', 'SGRUPHAB', 'PORTARIA']):
                df[col].replace('', None, inplace=True)

            # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes...
            # ("NaT") pela data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() \
                                        if x != '' else datetime(2099, 1, 1).date())
            df['DTPORTAR'] = df['DTPORTAR'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date() \
                                                  if x != '' else datetime(2099, 1, 1).date())

            # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
            # pela data futura "2099-01-01"
            for col in np.array(['CMPT_INI', 'CMPT_FIM', 'DTPORTAR', 'MAPORTAR']):
                df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                        datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['TERCEIRO', 'VINC_SUS']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas que são foreign keys
            df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID',
                                          'TPGESTAO': 'TPGESTAO_ID', 'PF_PJ': 'PFPJ_ID',
                                          'NIV_DEP': 'NIVDEP_ID', 'ESFERA_A': 'ESFERAA_ID',
                                          'RETENCAO': 'RETENCAO_ID', 'ATIVIDAD': 'ATIVIDAD_ID',
                                          'NATUREZA': 'NATUREZA_ID', 'CLIENTEL': 'CLIENTEL_ID',
                                          'TP_UNID': 'TPUNID_ID', 'TURNO_AT': 'TURNOAT_ID',
                                          'NIV_HIER': 'NIVHIER_ID', 'TP_PREST': 'TPPREST_ID',
                                          'NAT_JUR': 'NATJUR_ID'}, inplace=True)

            print(f'Tratou o arquivo IN{self.state}{self.year}{self.month} (shape final: {df.shape[0]} x {df.shape[1]}).')

            return df


# Classe de dados auxiliares do CNES
class DataCnesAuxiliary:

    # Construtor
    def __init__(self, path):
        self.path = path


    #######################################################################################################################
    #  CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES CNES #
    #######################################################################################################################
    # Método para adequar e formatar as colunas e valores das 27 Tabelas CADGERXX (arquivos CADGERXX.dbf),...
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
                            'COD_CEP', 'TELEFONE', 'FAX', 'EMAIL', 'REGSAUDE', 'MICR_REG',
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
        # Substitui o objeto datetime.date "9999-12-31" das duas colunas de "datas" pelo objeto datetime.date "2099-01-01"
        for col in np.array(['DATAINCL', 'DATAEXCL']):
            df[col].replace(datetime(9999, 12, 31).date(), datetime(2099, 1, 1).date(), inplace=True)
        # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui...
        # pela data futura "2099-01-01"
        for col in np.array(['DATAINCL', 'DATAEXCL']):
            df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                    datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

        # Upload do arquivo "xlsx" que contém os CNES presentes nos arquivos STXXaamm (dos anos de 2008 a 2019) e...
        # pela data futura "2099-01-01" nas 27 Tabelas CADGERXX. Ou seja, isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CNES_OUT_CADGER_XX_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar...
        # uma "string" de tamanho = 7
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(7))
        # Adiciona as colunas DESCESTAB, RSOC_MAN, CPF_CNPJ, EXCLUIDO, DATAINCL e DATAEXCL e respectivos valores...
        # ao objeto "dataframe"
        dataframe['DESCESTAB'] = 'NAO PROVIDO EM 27 ARQUIVOS DBF DE CNES'
        dataframe['RSOC_MAN'] = '?'
        dataframe['CPF_CNPJ'] = '?'
        dataframe['EXCLUIDO'] = None
        dataframe['DATAINCL'] = datetime(2099, 1, 1)
        dataframe['DATAEXCL'] = datetime(2099, 1, 1)
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


    # Método para adequar e formatar as colunas e valores da Tabela TABUF (arquivo TABUF.dbf)
    def get_TABUF_treated(self):
        # Conversão da Tabela TABUF para um objeto pandas DataFrame
        file_name = 'TABUF'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'ESTADO'}, inplace=True)
        # Reordena as colunas
        df = df[['ID', 'ESTADO', 'SIGLA_UF']]
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela codufmun
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


    # Método para adequar e formatar as colunas e valores da TCC TP_PFPJ (arquivo TP_PFPJ.cnv)
    def get_TP_PFPJ_treated(self):
        # Conversão da TCC TP_PFPJ para um objeto pandas DataFrame
        file_name = 'TP_PFPJ'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'PESSOA'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC NIVELDEP (arquivo NIVELDEP.cnv)
    def get_NIVELDEP_treated(self):
        # Conversão da TCC NIVELDEP para um objeto pandas DataFrame
        file_name = 'NIVELDEP'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC TPGESTAO (arquivo TPGESTAO.cnv)
    def get_TPGESTAO_treated(self):
        # Conversão da TCC TPGESTAO para um objeto pandas DataFrame
        file_name = 'TPGESTAO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'GESTAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC EsferAdm (arquivo EsferAdm.cnv)
    def get_EsferAdm_treated(self):
        # Conversão da TCC EsferAdm para um objeto pandas DataFrame
        file_name = 'EsferAdm'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ADMINISTRACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC RETENCAO (arquivo RETENCAO.cnv)
    def get_RETENCAO_treated(self):
        # Conversão da TCC RETENCAO para um objeto pandas DataFrame
        file_name = 'RETENCAO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'RETENCAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC Ativ_Ens (arquivo Ativ_Ens.cnv)
    def get_Ativ_Ens_treated(self):
        # Conversão da TCC Ativ_Ens para um objeto pandas DataFrame
        file_name = 'Ativ_Ens'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ATIVIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC NATUREZA (arquivo NATUREZA.cnv)
    def get_NATUREZA_treated(self):
        # Conversão da TCC NATUREZA para um objeto pandas DataFrame
        file_name = 'NATUREZA'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado
        df = df.drop(df[df['ID']=='0'].index)
        # Reset o index devido à exclusão efetuada no passo anterior
        df.reset_index(drop=True, inplace=True)
        # Coleta da coluna NATUREZA apenas a substring depois de dois dígitos e um traço
        df1 = df['NATUREZA'].str.extract('^\d{2}-(.*)', expand=True).rename(columns={0:'NATUREZA'})
        # Concatena ao longo do eixo das colunas os objetos pandas DataFrame "df[['ID']]" e "df1"
        dfinal = pd.concat([df[['ID']], df1], axis=1)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Método para adequar e formatar as colunas e valores da TCC Flux_Cli (arquivo Flux_Cli.cnv)
    def get_Flux_Cli_treated(self):
        # Conversão da TCC Flux_Cli para um objeto pandas DataFrame
        file_name = 'Flux_Cli'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CLIENTELA'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC TP_ESTAB (arquivo TP_ESTAB.cnv)
    def get_TP_ESTAB_treated(self):
        # Conversão da TCC TP_ESTAB para um objeto pandas DataFrame
        file_name = 'TP_ESTAB'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC TurnosAt (arquivo TurnosAt.cnv)
    def get_TurnosAt_treated(self):
        # Conversão da TCC TurnosAt para um objeto pandas DataFrame
        file_name = 'TurnosAt'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TURNO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC NIV_HIER (arquivo NIV_HIER.cnv)
    def get_NIV_HIER_treated(self):
        # Conversão da TCC NIV_HIER para um objeto pandas DataFrame
        file_name = 'NIV_HIER'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'NIVEL'}, inplace=True)
        # Coleta da coluna NIVEL apenas a substring depois de um traço
        df1 = df['NIVEL'].str.extract('^NH \d-(.*)', expand=True).rename(columns={0:'NIVEL'})
        # Concatena ao longo do eixo das colunas os objetos pandas DataFrame "df[['ID']]" e "df1"
        dfinal = pd.concat([df[['ID']], df1], axis=1)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Método para adequar e formatar as colunas e valores da TCC TIPOPRES (arquivo TIPOPRES.cnv)
    def get_TIPOPRES_treated(self):
        # Conversão da TCC TIPOPRES para um objeto pandas DataFrame
        file_name = 'TIPOPRES'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'PRESTADOR'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC NATJUR (arquivo NATJUR.cnv)
    def get_NATJUR_treated(self):
        # Conversão da TCC NATJUR para um objeto pandas DataFrame
        file_name = 'NATJUR'
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


    ###################################################################################################################
    # CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da TCC RETENMAN (arquivo RETENMAN.cnv)
    def get_RETENMAN_treated(self):
        # Conversão da TCC RETENMAN para um objeto pandas DataFrame
        file_name = 'RETENMAN'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'RETENCAO'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado
        df = df.drop(df[df['ID']=='IR'].index)
        # Reset o index devido à exclusão efetuada no passo anterior
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC ORGEXPED (arquivo ORGEXPED.cnv)
    def get_ORGEXPED_treated(self):
        # Conversão da TCC ORGEXPED para um objeto pandas DataFrame
        file_name = 'ORGEXPED'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'EXPEDIDOR'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC CLASAVAL (arquivo CLASAVAL.cnv)
    def get_CLASAVAL_treated(self):
        # Conversão da TCC CLASAVAL para um objeto pandas DataFrame
        file_name = 'CLASAVAL'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'AVALIACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores das Tabelas MEDIC_02, NV_SUP_02, TECNIC_02 e CBO_02...
    # (arquivos MEDIC_02.dbf, NV_SUP_02.dbf, TECNIC_02.dbf e CBO_02.dbf)
    def get_CBO_treated(self):
        # Conversão da Tabela MEDIC_02 para um objeto pandas DataFrame
        file_name = 'MEDIC_02'
        df1 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
        df1.sort_values(by=['ID'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df1.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df1.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df1" até formar uma...
        # "string" de tamanho = 6
        df1['ID'] = df1['ID'].apply(lambda x: x.zfill(6))
        # Conversão da Tabela NV_SUP_02 para um objeto pandas DataFrame
        file_name = 'NV_SUP_02'
        df2 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df2.rename(index=str, columns={'CO_CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
        # Ordena as linhas de "df2" por ordem crescente dos valores da coluna ID
        df2.sort_values(by=['ID'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df2.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df2.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df2" até formar uma...
        # "string" de tamanho = 6
        df2['ID'] = df2['ID'].apply(lambda x: x.zfill(6))
        # Conversão da Tabela TECNIC_02 para um objeto pandas DataFrame
        file_name = 'TECNIC_02'
        df3 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df3.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
        # Ordena as linhas de "df3" por ordem crescente dos valores da coluna ID
        df3.sort_values(by=['ID'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df3.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df3.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df3" até formar uma...
        # "string" de tamanho = 6
        df3['ID'] = df3['ID'].apply(lambda x: x.zfill(6))
        # Conversão da Tabela CBO_02 para um objeto pandas DataFrame
        file_name = 'CBO_02'
        df4 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df4.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df4['OCUPACAO'] = df4['OCUPACAO'].apply(lambda x: x.upper())
        # Ordena as linhas de "df4" por ordem crescente dos valores da coluna ID
        df4.sort_values(by=['ID'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df4.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df4.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df4" até formar uma...
        # "string" de tamanho = 6
        df4['ID'] = df4['ID'].apply(lambda x: x.zfill(6))
        # Concatena os quatro objetos pandas DataFrame
        frames = []
        frames.append(df1)
        frames.append(df2)
        frames.append(df3)
        frames.append(df4)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna "ID"
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os CBO presentes nos arquivos PFXXaamm (dos anos de 2006 a...
        # 2019) e não presentes nas Tabelas MEDIC_02, NV_SUP_02, TECNIC_02 e CBO_02. Ou seja, isso parece...
        # ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CBO_OUT_4_DBF_ANOS_2006_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até...
        # formar uma "string" de tamanho = 6
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(6))
        # Adiciona a coluna "OCUPACAO" e respectivos valores ao objeto "dataframe"
        dataframe['OCUPACAO'] = ['NAO PROVIDO EM 4 ARQUIVOS DBF DE CBO'] * (dataframe.shape[0])
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


    # Função para adequar e formatar as colunas e valores da Tabela CR_CONSEL (arquivo CR_CONSEL.dbf)
    def get_CR_CONSEL_treated(self):
        # Conversão da Tabela CR_CONSEL para um objeto pandas DataFrame
        file_name = 'CR_CONSEL'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CO_CONSE': 'ID', 'DS_CONSE': 'DENOMINACAO'}, inplace=True)
        # Drop as linhas inteiras em que a coluna "ID" tem o valor especificado por não representar conselho algum
        df = df.drop(df[df['ID']=='99'].index)
        df = df.drop(df[df['ID']==''].index)

        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)

        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela VINCULO (arquivo VINCULO.dbf)
    def get_VINCULO_treated(self):
        # Conversão da Tabela VINCULO para um objeto pandas DataFrame
        file_name = 'VINCULO'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CO_VINC': 'ID', 'DS_VINC': 'DESCRICAO'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nenhum...
        # tipo de vínculo
        df = df.drop(df[df['ID']=='000000'].index)

        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)

        # Insere três linhas devido oa formato antigo da coluna VINCULAC de um único dígito variando de "1" a "3"
        df.loc[df.shape[0]] = ['000001', 'PROFISSIONAL CONTRATADO']
        df.loc[df.shape[0]] = ['000002', 'PROFISSIONAL AUTÔNOMO']
        df.loc[df.shape[0]] = ['000003', 'PROFISSIONAL VÍNCULO NÃO IDENTIFICADO']

        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" até formar uma...
        # "string" de tamanho = 6
        df['ID'] = df['ID'].apply(lambda x: x.zfill(6))
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT CNES_LT #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da TCC tip1leit (arquivo tip1leit.cnv)
    def get_tip1leit_treated(self):
        # Conversão da TCC tip1leit para um objeto pandas DataFrame
        file_name = 'tip1leit'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC Esp_leit (arquivo Esp_leit.cnv)
    def get_Esp_leit_treated(self):
        # Conversão da TCC Esp_leit para um objeto pandas DataFrame
        file_name = 'Esp_leit'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESPECIALIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ CNES_EQ #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da Tabela TP_EQUIPAM (arquivo TP_EQUIPAM.dbf)
    def get_TP_EQUIPAM_treated(self):
        # Conversão da Tabela TP_EQUIPAM para um objeto pandas DataFrame
        file_name = 'TP_EQUIPAM'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CHAVE': 'ID', 'DS_TPEQUIP': 'TIPO'}, inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC Equip_Tp (arquivo Equip_Tp.cnv)
    def get_Equip_Tp_treated(self):
        # Conversão da TCC Equip_Tp para um objeto pandas DataFrame
        file_name = 'Equip_Tp'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'DENOMINACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR CNES_SR #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores das tabelas S_CLASSEN e SRA_ORD_N (arquivos S_CLASSEN.dbf...
    # e SRA_ORD_N.dbf)
    def get_SERVICO_treated(self):
        # Conversão da Tabela S_CLASSEN para um objeto pandas DataFrame
        file_name = 'S_CLASSEN'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CHAVE': 'ID', 'DS_SERV': 'DESCRICAO'}, inplace=True)
        # Coleta apenas os três primeiros dígitos da coluna ID do objeto pandas DataFrame "df"
        df1 = df['ID'].str.extract('(^\d{3})', expand=True).rename(columns={0:'ID'})
        # Coleta da coluna DESCRICAO apenas a substring antes da barra
        df2 = df['DESCRICAO'].str.extract('^\d{3} (.*) /', expand=True).rename(columns={0:'DESCRICAO'})
        # Concatena lado a lado os objetos pandas DataFrame "df1" e "df2"
        df3 = pd.concat([df1, df2], axis=1)
        # Elimina as linhas duplicadas do objeto pandas DataFrame "df3" tendo por base a coluna ID e mantém a...
        # primeira ocorrência
        df3.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido à exclusão das linhas efetuada no passo anterior
        df3.reset_index(drop=True, inplace=True)
        # Conversão da Tabela SRA_ORD_N para um objeto pandas DataFrame
        file_name = 'SRA_ORD_N'
        df4 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df4.rename(index=str, columns={'CHAVE': 'ID', 'DS_SERV': 'DESCRICAO'}, inplace=True)
        # Torna UPPER CASE todos os valores da coluna DESCRICAO do objeto pandas DataFrame "df4"
        df4['DESCRICAO'] = df4['DESCRICAO'].apply(lambda x: x.upper())
        # Concatena um sobre o outro os objetos pandas DataFrame "df3" e "df4"
        frames = []
        frames.append(df3)
        frames.append(df4)
        dfinal = pd.concat(frames, axis=0, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna "ID"
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Método para adequar e formatar as colunas e valores das tabelas S_CLASSEN e S_CLASSEA (arquivos...
    # S_CLASSEN.dbf e S_CLASSEA.dbf)
    def get_CLASSSR_treated(self):
        # Conversão da Tabela S_CLASSEN para um objeto pandas DataFrame
        file_name = 'S_CLASSEN'
        df = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df.rename(index=str, columns={'CHAVE': 'ID', 'DS_SERV': 'DESCRICAO'}, inplace=True)
        # Coleta da coluna DESCRICAO apenas a substring depois de três dígitos
        df1 = df['DESCRICAO'].str.extract('^\d{3} (.*)', expand=True).rename(columns={0:'DESCRICAO'})
        # Concatena lado a lado os objetos pandas DataFrame "df[['ID']]" e "df1"
        df2 = pd.concat([df[['ID']], df1], axis=1)
        # Conversão da Tabela S_CLASSEA para um objeto pandas DataFrame
        file_name = 'S_CLASSEA'
        df3 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df3.rename(index=str, columns={'CHAVE': 'ID', 'DS_SERV': 'DESCRICAO'}, inplace=True)
        # Desconsidera as linhas do objeto pandas DataFrame que têm na coluna DESCRICAO as substring discriminadas
        df3 = df3[~df3['DESCRICAO'].str.contains('|'.join(['^.*Sem definicao', '^.*Classificacao nao informada',
                                                           '^.*inexistente.*', '^.*Sem classificacao']), regex=True)]
        # Reset o index devido à eliminação de linhas efetuadas nos passos anteriores
        df3.reset_index(drop=True, inplace=True)
        # Coleta da coluna DESCRICAO apenas a substring depois do "("
        df4 = df3['DESCRICAO'].str.extract('^Servico - \d{3} / \d{3} - (.*)', \
                                           expand=True).rename(columns={0:'DESCRICAO'})
        # Concatena lado a lado os objetos pandas DataFrame "df3[['ID']]" e "df4"
        df5 = pd.concat([df3[['ID']], df4], axis=1)
        # Torna UPPER CASE todos os valores da coluna DESCRICAO do objeto pandas DataFrame "df5"
        df5['DESCRICAO'] = df5['DESCRICAO'].apply(lambda x: x.upper())
        # Concatena um sobre o outro os objetos pandas DataFrame "df2" e "df5"
        frames = []
        frames.append(df2)
        frames.append(df5)
        dfinal = pd.concat(frames, axis=0, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna "ID"
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Método para adequar e formatar as colunas e valores da TCC Srv_Caract (arquivo Srv_Caract.cnv)
    def get_Srv_Caract_treated(self):
        # Conversão da TCC Srv_Caract para um objeto pandas DataFrame
        file_name = 'Srv_Caract'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CARACTERIZACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores das Tabelas EQP_XX (arquivos EQP_XX.dbf, sendo XX a...
    # sigla do Estado da RFB)
    def get_EQP_XX_treated(self):
        # Lista de Estados da RFB
        estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                            'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
        frames = []
        for uf in estados:
            # Conversão da Tabela EQP_XX para um objeto pandas DataFrame
            file_name = 'EQP_' + uf
            df = download_table_dbf(file_name)
            # Renomeia colunas especificadas
            df.rename(index=str, columns={'IDEQUIPE': 'ID', 'NOME_EQP': 'NOME_EQUIPE'}, inplace=True)
            # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
            df.drop_duplicates(subset='ID', keep='first', inplace=True)
            # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
            df.sort_values(by=['ID'], inplace=True)
            # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
            df.reset_index(drop=True, inplace=True)
            # Acrescenta "df" como um elemento de um objeto list
            frames.append(df)
        # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
        df = pd.concat(frames, axis=0, ignore_index=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os IDEQUIPE presentes nos arquivos EPXXaamm (dos anos de 2008 a...
        # 2019) e não presentes nas 27 Tabelas EQP_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'IDEQUIPE_OUT_27_EQP_XX_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona a coluna "NOME_EQUIPE" e respectivos valores ao objeto "dataframe"
        dataframe['NOME_EQUIPE'] = ['NAO PROVIDO NOS 27 EQP_XX.DBF'] * (dataframe.shape[0])
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


    # Método para adequar e formatar as colunas e valores da Tabela EQUIPE (arquivo EQUIPE.dbf)
    def get_EQUIPE_treated(self):
        # Conversão da Tabela EQUIPE para um objeto pandas DataFrame
        file_name = 'EQUIPE'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'TP_EQUIPE': 'ID', 'DS_EQUIPE': 'TIPO'}, inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores das Tabelas AREA_XX (arquivos AREA_XX.dbf, sendo XX...
    # a sigla do Estado da RFB)
    def get_AREA_XX_treated(self):
        # Lista de Estados da RFB
        estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                            'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
        frames = []
        for uf in estados:
            # Conversão da Tabela AREA_XX para um objeto pandas DataFrame
            file_name = 'AREA_' + uf
            df = download_table_dbf(file_name)
            # Renomeia colunas especificadas
            df.rename(index=str, columns={'ID_AREA': 'ID', 'NOMEAREA': 'NOME_AREA'}, inplace=True)
            # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
            df.drop_duplicates(subset='ID', keep='first', inplace=True)
            # Desconsidera as linhas de "df" que têm na coluna NOME_AREA a substring discriminada
            df = df[~df['NOME_AREA'].str.contains('AREA NAO INFORMADA', regex=True)]
            # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
            df.sort_values(by=['ID'], inplace=True)
            # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
            df.reset_index(drop=True, inplace=True)
            # Acrescenta "df" como um elemento de um objeto list
            frames.append(df)
        # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
        df = pd.concat(frames, axis=0, ignore_index=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os ID_AREA presentes nos arquivos EPXXaamm (dos anos de 2008 a...
        # 2019) e não presentes nas 27 Tabelas AREA_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'ID_AREA_OUT_27_AREA_XX_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona a coluna "NOME_AREA" e respectivos valores ao objeto "dataframe"
        dataframe['NOME_AREA'] = ['NAO PROVIDO NOS 27 AREA_XX.DBF'] * (dataframe.shape[0])
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


    # Método para adequar e formatar as colunas e valores das Tabelas SEGM_XX (arquivos SEGM_XX.dbf, sendo XX...
    # a sigla do Estado da RFB)
    def get_SEGM_XX_treated(self):
        # Lista de Estados da RFB
        estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                            'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
        frames = []
        for uf in estados:
            # Conversão da Tabela SEGM_XX para um objeto pandas DataFrame
            file_name = 'SEGM_' + uf
            df = download_table_dbf(file_name)
            # Renomeia colunas especificadas
            df.rename(index=str, columns={'ID_SEGM': 'ID', 'DESCSEGM': 'DESCRICAO'}, inplace=True)
            # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
            df.drop_duplicates(subset='ID', keep='first', inplace=True)
            # Desconsidera as linhas de "df" que têm na coluna NOME_AREA a substring discriminada
            df = df[~df['DESCRICAO'].str.contains('SEGMENTO NAO INFORMADO', regex=True)]
            # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
            df.sort_values(by=['ID'], inplace=True)
            # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
            df.reset_index(drop=True, inplace=True)
            # Acrescenta "df" como um elemento de um objeto list
            frames.append(df)
        # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
        df = pd.concat(frames, axis=0, ignore_index=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os ID_SEGM presentes nos arquivos EPXXaamm (dos anos de 2008 a...
        # 2019) e não presentes nas 27 Tabelas SEGM_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'ID_SEGM_OUT_27_SEGM_XX_ANOS_2008_2019' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona a coluna "DESCRICAO" e respectivos valores ao objeto "dataframe"
        dataframe['DESCRICAO'] = ['NAO PROVIDO NOS 27 SEGM_XX.DBF'] * (dataframe.shape[0])
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


    # Método para adequar e formatar as colunas e valores da TCC tiposegm (arquivo tiposegm.cnv)
    def get_tiposegm_treated(self):
        # Conversão da TCC tiposegm para um objeto pandas DataFrame
        file_name = 'tiposegm'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC motdesat (arquivo motdesat.cnv)
    def get_motdesat_treated(self):
        # Conversão da TCC motdesat para um objeto pandas DataFrame
        file_name = 'motdesat'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'MOTIVO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da TCC TP_DESAT (arquivo TP_DESAT.cnv)
    def get_TP_DESAT_treated(self):
        # Conversão da TCC TP_DESAT para um objeto pandas DataFrame
        file_name = 'TP_DESAT'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB CNES_HB #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da Tabela HABILITA (arquivo HABILITA.dbf)
    def get_HABILITA_treated(self):
        # Conversão da Tabela HABILITA para um objeto pandas DataFrame
        file_name = 'HABILITA'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CD_HABILIT': 'ID', 'DS_HABIL': 'HABILITACAO'}, inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC CNES_RC #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da Tabela REGRAS (arquivo REGRAS.dbf)
    def get_REGRAS_treated(self):
        # Conversão da Tabela REGRAS para um objeto pandas DataFrame
        file_name = 'REGRAS'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CHAVE': 'ID', 'DS_REGRA': 'REGRA'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df['REGRA'] = df['REGRA'].apply(lambda x: x.upper())
        # Desconsidera a linha de "df" que têm na coluna REGRA a substring discriminada
        df = df[~df['REGRA'].str.contains('SEM REGRA CONTRATUAL', regex=True)]
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM CNES_GM #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da Tabela GESTAO (arquivo GESTAO.dbf)
    def get_GESTAO_treated(self):
        # Conversão da Tabela GESTAO para um objeto pandas DataFrame
        file_name = 'GESTAO'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CD_GESTAO': 'ID', 'DS_GESTAO': 'GESTAO'}, inplace=True)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df = df.drop(['TP_GESTAO', 'VIGENCIA_I', 'VIGENCIA_F'], axis=1)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df['GESTAO'] = df['GESTAO'].apply(lambda x: x.upper())
        # Substitui em determinados valores da coluna GESTAO uma substring por outra
        df['GESTAO'].replace(regex=['^ESTABELEIC'], value='ESTABELECI', inplace=True)
        df['GESTAO'].replace(regex=['^ESTABELECIMENTOS'], value='ESTABELECIMENTO', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE CNES_EE #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da TCC ESTABENS (arquivo ESTABENS.cnv)
    def get_ESTABENS_treated(self):
        # Conversão da TCC ESTABENS para um objeto pandas DataFrame
        file_name = 'ESTABENS'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    ###################################################################################################################
    # CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF CNES_EF #
    ###################################################################################################################
    # Método para adequar e formatar as colunas e valores da TCC ESTABFIL (arquivo ESTABFIL.cnv)
    def get_ESTABFIL_treated(self):
        # Conversão da TCC ESTABFIL para um objeto pandas DataFrame
        file_name = 'ESTABFIL'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Ajustar "the_path" para a localização dos arquivos "xlsx"
    the_path = os.getcwd()[:-len('insertion\\data_wrangling')] + 'files\\CNES\\'



    print(df)

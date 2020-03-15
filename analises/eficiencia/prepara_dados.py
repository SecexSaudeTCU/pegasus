"""
Prepara planilhas para clusterização e análise DEA.
    Origem dos dados:
     - A planilha do Marcelo é utilizadas como fonte de dados para clusterização
     - As planilhas do Eric são utilizadas como fonte das entradas e saídas para DEA
     - Os valores de produção registrados no SIA e no SIH foram obtidos pela soma de todos os meses do respectivo ano
     - Os establecimentos e demais variáveis (i.e. variáveis do CNES) foram obtidas do mês de dezembro de cada ano

    Exclusão e inclusão de unidades (valores de 2018):
     - Unidades não hospitalares são removidas (sobram 7.745 de um total original de 331.058)
     - Unidades não públicas são removidas (sobram 3.304)
     - Unidades geridas por OSS (da lista do IBROSS) são adicionadas (aumenta para 3.308)
"""


import os
import pandas as pd
import pandas_profiling

if __name__ == '__main__':

    # Ano dos dados para DEA (para fis de clusterização, estamos sempre utilizando os dados de 2018)
    ANO = '2019'

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_ORIGINAIS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais')
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')

    # Carrega dados da planilha gerada pelo Eric
    arquivo_dados = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'd{ANO}.xlsx'.format(ANO=ANO))
    df_tudo = pd.read_excel(arquivo_dados)

    # Mapeamento entre primeiro dígito do código da natureza jurídica e o tipo de natureza jurídica
    map_nat_jur = {'1': 'Administração Pública', '2': 'Entidades Empresariais', '3': 'Entidades sem Fins Lucrativos',
                   '4': 'Pessoas Físicas', '5': 'Organizações Internacionais e Outras Instituições Extraterritoriais'}
    # Adiciona coluna com natureza jurídica simplificada
    df_tudo['NAT_JURIDICA_SIMPLIFICADA'] = [map_nat_jur[n] for n in df_tudo.NAT_JURIDICA.str[0]]



    # Lista de tipos constantes da coluna TPPREST que serão mantidos (sobram 7.912 de 312.783)
    # (todos têm ao menos 9 unidades com  mais de 24 leitos).
    print('Início', df_tudo.shape)
    tipos_unidades_mantidas = ['HOSPITAL GERAL', 'HOSPITAL ESPECIALIZADO', 'UNIDADE MISTA', 'PRONTO SOCORRO GERAL',
                               'PRONTO SOCORRO ESPECIALIZADO', 'HOSPITAL/DIA - ISOLADO']
    # Mantém somente unidades de caráter hospitalar
    df_hosp = df_tudo[df_tudo.TIPO.isin(tipos_unidades_mantidas)]
    print('Após remover não-hospitais', df_hosp.shape)







    # Remove unidades não públicas (sobram 3.330 de 7.912)
    df_hosp_pubs = df_hosp[df_hosp.NAT_JURIDICA_SIMPLIFICADA == 'Administração Pública']
    print('Após remover não públicas', df_hosp_pubs.shape)





    # Carrega lista de hospitais geridos por OSS de acordo com a lista do IBROSS
    arquivo_lista_ibross = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'lista_ibross.xlsx')
    lista_cnes_oss = pd.read_excel(arquivo_lista_ibross).CNES.to_list()

    # Cria df somente com as unidades geridas por OSS
    df_oss = df_tudo[df_tudo.CNES_ID.isin(lista_cnes_oss)]

    # Inclui OSS no df e remove linhas duplicadas (pois as OSS "púlicas" já estavam na lista)
    df_hosp_pubs_oss = pd.concat((df_hosp_pubs, df_oss))
    df_hosp_pubs_oss = df_hosp_pubs_oss.drop_duplicates(subset='CNES_ID', keep='first')
    print('Após incluir OSS', df_hosp_pubs_oss.shape)

    # Adiciona coluna indicando se a unidade é gerida por OSS
    df_hosp_pubs_oss['SE_GERIDA_OSS'] = ['SIM' if cnes in lista_cnes_oss else 'NA' for cnes in df_hosp_pubs_oss.CNES_ID]

    # Adiciona a coluna com o valor total da produção do hospital, incluindo SIA e SIH
    # TODO: reavaliar a inclusão dos valores da UTI (coluna VAL_UTI do CNES)
    df_hosp_pubs_oss['SIA_SIH_VALOR'] = df_hosp_pubs_oss['VALOR_SIA_TOTAL'] + df_hosp_pubs_oss['VALOR_AIH_TOTAL']





    # Renomeia e remove colunas
    df_hosp_pubs_oss = df_hosp_pubs_oss.rename(columns={
        'CNES_ID': 'CNES',
        'MUNNOME': 'MUNICIPIO',
        'ATIVIDADE': 'ATIVIDADE_ENSINO',
        'CNES_MEDICOS (PELO CNS)': 'CNES_MEDICOS',
        'CNES_PROFISSIONAIS_ENFERMAGEM (PELO CNS)': 'CNES_PROFISSIONAIS_ENFERMAGEM',
        'TIPO': 'TIPO_UNIDADE'
    })
    df_hosp_pubs_oss = df_hosp_pubs_oss.drop(columns=[
        'CNES_MEDICOS (PELO NOME)',  # Estamos usando o CNS para determinar o o profissional
        'CNES_PROFISSIONAIS_ENFERMAGEM (PELO NOME)',  # Idem
        'GESTAO',  # Talvez esta coluna seja interessante para comparar OSS gerida por Estado x Municio
        'PRESTADOR',  # Coluna somente com NA,
        'VALOR_SIA_TOTAL',
        'VALOR_AIH_TOTAL',
        'VALOR_UTI'
    ])

    # Reordena  colunas
    ordem_colunas = [
        'CNES', 'UF', 'MUNICIPIO', 'NAT_JURIDICA_SIMPLIFICADA', 'NAT_JURIDICA', 'TIPO_UNIDADE', 'SE_GERIDA_OSS',
        'CNES_LEITOS_SUS', 'CNES_MEDICOS', 'CNES_PROFISSIONAIS_ENFERMAGEM', 'CNES_SALAS',
        'SIA_SIH_VALOR',
    ]
    df_hosp_pubs_oss = df_hosp_pubs_oss[ordem_colunas]

    # Salva planilha para DEA
    #arquivo_para_dea = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'hosp_pubs_e_oss_{ANO}.xlsx'.format(ANO=ANO))
    #df_hosp_pubs_oss.to_excel(arquivo_para_dea, index=False)

    # Salva profile em html
    #pp = pandas_profiling.ProfileReport(df_hosp_pubs_oss, title='Hospitais públicos e geridos por OSS (lista IBROSS)')
    #pp.to_file('Hospitais públicos e geridos por OSS ({ANO}).html'.format(ANO=ANO))











    # Carrega arquivo com perfil de procedimentos dos hospitais para clusterização
    arquivo_unidades_perfil = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'unidades_perfil_todos_dez_2018.csv')
    df_perfil = pd.read_csv(arquivo_unidades_perfil, sep=';', decimal=',')
    print('Perfil todos', df_perfil.shape)

    # Verica se todos os hospitais da amostra possuem um perfil (isto deve ser verdadeiro para 2018)
    # assert sum(df_hosp_pubs_oss.CNES.isin(df_perfil.N_CNES)) == len(df_hosp_pubs_oss)
    print('Nº de linhas encontradas na planilha para DEA', len(df_hosp_pubs_oss))
    print('Nº de linhas correspondentes encontradas na planilha de perfil', df_hosp_pubs_oss.CNES.isin(df_perfil.N_CNES).sum())

    # Cria DF com os hospitais públicos e geridos por OSS com os respectivos perfils de procedimetos
    df_hosp_pubs_oss_com_perfil = pd.merge(df_hosp_pubs_oss, df_perfil, left_on='CNES', right_on='N_CNES', how='left')
    df_hosp_pubs_oss_com_perfil = df_hosp_pubs_oss_com_perfil.drop(columns=['N_CNES'])

    # Remove hospitais com NAs (estes NAs vem dos perfils)
    df_hosp_pubs_oss_com_perfil = df_hosp_pubs_oss_com_perfil.dropna()
    print('Nº de linhas na planilha final', len(df_hosp_pubs_oss_com_perfil))

    # Salva arquivo para clusterização (o perfil é do ano inteiro e demais variáveis só de dezembro)
    arquivo_final = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'perfil_hosp_pubs_oss_dez_{ANO}.xlsx'.format(ANO=ANO))
    df_hosp_pubs_oss_com_perfil.to_excel(arquivo_final, index=False)


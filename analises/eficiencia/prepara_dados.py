"""
Prepara planilhas para clusterização e análise DEA.
    Origem dos dados:
     - Os valores de produção registrados no SIA e no SIH foram obtidos pela soma de todos os meses do respectivo ano
     - As demais variáveis e a lista de hospitais (i.e. lista de ids do CNES) foram obtidas do mês de dez de cada ano

    Exclusão e inclusão de unidades (valores de 2018):
     - Unidades não hospitalares são removidas (sobram 7.745 de um total original de 331.058)
     - Unidades não públicas são removidas (sobram 3.304)
     - Unidades com valor zero em variável utilizada pela DEA (sobram 2.861 de 3.304)
"""


import os
import pandas as pd
import pandas_profiling

if __name__ == '__main__':

    # Ano dos dados para DEA (para fis de clusterização, estamos sempre utilizando os dados de 2018)
    ANO = '2018'

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_ORIGINAIS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais')
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')

    # Carrega dados da planilha gerada pelo Eric
    arquivo_dados = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'd{ANO}.csv'.format(ANO=ANO))
    df_tudo = pd.read_csv(arquivo_dados)

    # Substiui nans pelo valor zero nos valores da produção do SIA e do SIH
    df_tudo.VALOR_SIA = df_tudo.VALOR_SIA.fillna(value=0)
    df_tudo.VALOR_SIH = df_tudo.VALOR_SIH.fillna(value=0)

    # Mapeamento entre primeiro dígito do código da natureza jurídica e o tipo de natureza jurídica
    map_nat_jur = {'1': 'Administração Pública', '2': 'Entidades Empresariais', '3': 'Entidades sem Fins Lucrativos',
                   '4': 'Pessoas Físicas', '5': 'Organizações Internacionais e Outras Instituições Extraterritoriais'}

    # Adiciona coluna com natureza jurídica simplificada (depois da coluna NATUREZA_JURIDICA)
    pos_col_depois_nat_jur = 1 + df_tudo.columns.to_list().index('NAT_JURIDICA')
    nat_jur_siplificada = [map_nat_jur[n] for n in df_tudo.NAT_JURIDICA.str[0]]
    df_tudo.insert(loc=pos_col_depois_nat_jur, column='NAT_JURIDICA_SIMPLIFICADA', value=nat_jur_siplificada)

    # Lista de tipos constantes da coluna TPPREST que serão mantidos
    # (todos têm ao menos 9 unidades com  mais de 24 leitos).
    print('Início', df_tudo.shape)
    tipos_unidades_mantidas = ['HOSPITAL GERAL', 'HOSPITAL ESPECIALIZADO', 'UNIDADE MISTA', 'PRONTO SOCORRO GERAL',
                               'PRONTO SOCORRO ESPECIALIZADO', 'HOSPITAL/DIA - ISOLADO']

    # Mantém somente unidades de caráter hospitalar
    df_hosp = df_tudo[df_tudo.TIPO.isin(tipos_unidades_mantidas)]
    print('Após remover não-hospitais', df_hosp.shape)

    # Remove unidades não públicas
    df_hosp_pubs = df_hosp[df_hosp.NAT_JURIDICA_SIMPLIFICADA == 'Administração Pública']
    print('Após remover não públicas', df_hosp_pubs.shape)

    # Carrega lista de hospitais geridos por OSS de acordo com a lista do IBROSS
    arquivo_lista_ibross = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'lista_ibross.xlsx')
    lista_cnes_oss = pd.read_excel(arquivo_lista_ibross).CNES.to_list()

    # Adiciona coluna indicando se a unidade é gerida por OSS
    se_gerida_oss = ['SIM' if cnes in lista_cnes_oss else 'NA' for cnes in df_hosp_pubs.CNES_ID]
    pos_col_apos_tipo_unidade = df_hosp_pubs.columns.to_list().index('TIPO')
    df_hosp_pubs.insert(loc=pos_col_apos_tipo_unidade, column='SE_GERIDA_OSS', value=se_gerida_oss)

    # Adiciona a coluna com o valor total da produção do hospital, incluindo SIA e SIH
    pos_col_antes_valor_sia = df_hosp_pubs.columns.to_list().index('VALOR_SIA')
    valor_total = df_hosp_pubs['VALOR_SIA'] + df_hosp_pubs['VALOR_SIH']
    df_hosp_pubs.insert(loc=pos_col_antes_valor_sia, column='SIA_SIH_VALOR', value=valor_total)

    # Renomeia e remove colunas
    df_hosp_pubs = df_hosp_pubs.rename(columns={
        'CNES_ID': 'CNES',
        'MUNNOME': 'MUNICIPIO',
        'ATIVIDADE': 'ATIVIDADE_ENSINO',
        'CNES_MEDICOS (PELO CNS)': 'CNES_MEDICOS',
        'CNES_PROFISSIONAIS_ENFERMAGEM (PELO CNS)': 'CNES_PROFISSIONAIS_ENFERMAGEM',
        'TIPO': 'TIPO_UNIDADE'
    })
    df_hosp_pubs = df_hosp_pubs.drop(columns=[
        'CNES_MEDICOS (PELO NOME)',  # Estamos usando o CNS para determinar o o profissional
        'CNES_PROFISSIONAIS_ENFERMAGEM (PELO NOME)',  # Idem
        'GESTAO',  # Talvez esta coluna seja interessante para comparar OSS gerida por Estado x Municio
        'PRESTADOR',  # Coluna somente com NA,
        'VALOR_SIA', 'VALOR_SIH', 'QTD_SIA', 'QTD_SIH'
    ])

    # Remove hospitais que possuem valor igual a zero para alguma das variáveis utilizadas na DEA
    print('Removendo linhas com valor zero nas colunas utilizadas para DEA')
    colunas_dea = ['CNES_LEITOS_SUS', 'CNES_SALAS', 'CNES_MEDICOS', 'CNES_PROFISSIONAIS_ENFERMAGEM', 'SIA_SIH_VALOR']
    for coluna in colunas_dea:
        mascara = df_hosp_pubs[coluna] == 0
        df_hosp_pubs = df_hosp_pubs[~mascara]
        print(' * {n} linhas revmovidas por conter zero na coluna {coluna}'.format(n=sum(mascara), coluna=coluna))
    print('Número de estabelcimentos ao final do processamento: {n}'.format(n=len(df_hosp_pubs)))

    # Verifica se ao final houve algum hospital somatória de proporções de procedimentos inferior a 0.99
    idx = df_hosp_pubs.columns.to_list().index('SIA-0101')
    num_com_soma_inferior_a_um = sum(df_hosp_pubs[df_hosp_pubs.columns[idx::]].sum(axis=1) < 0.999)
    assert(num_com_soma_inferior_a_um == 0)

    # Salva planilha para DEA
    arquivo_para_dea = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'hosp_pubs_{ANO}.xlsx'.format(ANO=ANO))
    df_hosp_pubs.to_excel(arquivo_para_dea, index=False)


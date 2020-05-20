from outliers.estatistica.zscore import ZScore, ZScoreModificado
from outliers.estatistica.iqr import InterquartileRange
from outliers.densidade.lof import LOF
from outliers.proximidade.isolation_forest import IsolationForest
from outliers.detector import DetectorOutlier
from sih.sih_facade import SIHFacade
import sys
from estatistica.normalidade import se_distribuicao_normal
import numpy as np
from scipy import stats
import pandas as pd
from config.configuracoes import ConfiguracoesAnalise

NOME_COLUNA = 'TX_QTD'

NAO_NORMAL = 'NÃO NORMAL'

AP_S_LOG = 'NORMAL APÓS LOG'

NORMAL_ORIGINAL = 'NORMAL ORIGINAL'

AP_S_BOXCOX = 'NORMAL APÓS BOXCOX'


# TODO: Implemenar também CompositeSIADetector e checar o que existe em comum em termos de lógica

class SIHCompositeDetectorOutlier(DetectorOutlier):
    def __init__(self, df, nome_coluna, distribuicao, qtd_std=3, quantidade_vizinhos=20):
        super(SIHCompositeDetectorOutlier, self).__init__(df, nome_coluna)
        self.distribuicao = distribuicao
        self.qtd_std = qtd_std
        self.quantidade_vizinhos = quantidade_vizinhos

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]

        print(df_pop.describe())

        if (self.distribuicao in (NORMAL_ORIGINAL, AP_S_LOG, AP_S_BOXCOX)):
            # metodos estatisticos
            # Z-score
            zscore_outliers = ZScore(self.df, self.nome_coluna, self.qtd_std).get_outliers()
            # Z-score modificado
            zscore_outliers2 = ZScoreModificado(self.df, self.nome_coluna, self.qtd_std).get_outliers()
            # IQR
            iqr_outliers = InterquartileRange(self.df, self.nome_coluna).get_outliers()

        # baseado em proximidade
        # Isolation Forest
        isolation_outliers = IsolationForest(self.df, self.nome_coluna).get_outliers()

        # baseado em densidade
        # LocalOutlierFactor
        lof_outliers = LOF(self.df, self.nome_coluna, self.quantidade_vizinhos).get_outliers()

        if (self.distribuicao in (NORMAL_ORIGINAL, AP_S_LOG, AP_S_BOXCOX)):
            comum_outliers = zscore_outliers & zscore_outliers2 & iqr_outliers & isolation_outliers & lof_outliers
        else:
            comum_outliers = isolation_outliers & lof_outliers
        outliers = sorted(set(comum_outliers))

        if len(outliers) > 0:
            if (self.distribuicao in (NORMAL_ORIGINAL, AP_S_LOG, 'NORMAL APÓS BOXCOX')):
                print('\nZ-Score: ' + str(len(zscore_outliers)),
                      '\nZ-Score modificado: ' + str(len(zscore_outliers2)),
                      '\nIQR: ' + str(len(iqr_outliers)),
                      '\nIsolation Forest: ' + str(len(isolation_outliers)),
                      '\nLocal Outlier Factor: ' + str(len(lof_outliers)),
                      '\nComum: ' + str(len(outliers)),
                      '\nOutliers Comuns (índices): ' + str(outliers) + '\n')
            else:
                print('\nIsolation Forest: ' + str(len(isolation_outliers)),
                      '\nLocal Outlier Factor: ' + str(len(lof_outliers)),
                      '\nComum: ' + str(len(outliers)),
                      '\nOutliers Comuns (índices): ' + str(outliers) + '\n')
        else:
            print('\nNão foram detectados outliers para esse procedimento!\n')

        return outliers


def __teste_normalidade(len_min, df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao):
    df_proc_analise_estatistica = pd.DataFrame(
        columns=['ANO', 'cod_municipio', 'PROCEDIMENTO', 'NIVEL', 'TX', 'TX_QTD', 'MEDIA', 'MEDIANA', 'STD',
                 'DISTRIBUICAO', 'MIN', 'MAX'])

    for procedimento in df_descricao_procedimentos.index.values:
        df_proc = __gerar_informacoes_estatisticas(df_procedimentos_por_ano_com_descricao, len_min, procedimento)
        df_proc_analise_estatistica = df_proc_analise_estatistica.append(df_proc)

    return df_proc_analise_estatistica


def get_df_distribuicao_nivel(len_min, df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao):
    df_proc_analise_estatistica = __teste_normalidade(len_min, df_descricao_procedimentos,
                                                      df_procedimentos_por_ano_com_descricao)
    df_analise = df_proc_analise_estatistica[['DISTRIBUICAO', 'NIVEL', 'PROCEDIMENTO']].drop_duplicates()
    print(df_analise.shape)

    grp_NORMAL_BOXCOX = df_analise[df_analise.DISTRIBUICAO == 'NORMAL APÓS BOXCOX'].groupby('NIVEL').count()[
        'PROCEDIMENTO'].reset_index()
    grp_NORMAL_LOG = df_analise[df_analise.DISTRIBUICAO == 'NORMAL APÓS LOG'].groupby('NIVEL').count()[
        'PROCEDIMENTO'].reset_index()
    grp_NAO_CHECADA = df_analise[df_analise.DISTRIBUICAO == 'NÃO CHECADA - AMOSTRA PEQUENA'].groupby('NIVEL').count()[
        'PROCEDIMENTO'].reset_index()
    grp_NAO_NORMAL = df_analise[df_analise.DISTRIBUICAO == 'NÃO NORMAL'].groupby('NIVEL').count()[
        'PROCEDIMENTO'].reset_index()

    df_distribuicao_nivel1 = grp_NORMAL_BOXCOX.merge(grp_NORMAL_LOG, on='NIVEL', how='outer',
                                                     suffixes=('_NORMAL_BOXCOX', '_NORMAL_LOG')).fillna(0).sort_values(
        by='NIVEL')
    df_distribuicao_nivel2 = grp_NAO_NORMAL.merge(grp_NAO_CHECADA, on='NIVEL', how='outer',
                                                  suffixes=('_NAO_NORMAL', '_NAO_CHECADA')).fillna(0).sort_values(
        by='NIVEL')
    df_distribuicao_nivel = df_distribuicao_nivel1.merge(df_distribuicao_nivel2, on='NIVEL', how='outer',
                                                         suffixes=(False, False)).fillna(0).sort_values(by='NIVEL')
    print(df_distribuicao_nivel.sum())
    print(df_distribuicao_nivel.head())

    return df_distribuicao_nivel


def __gerar_informacoes_estatisticas(df_procedimentos_por_ano_com_descricao, len_min, procedimento):
    # Obtendo dados do procedimento
    df_proc = df_procedimentos_por_ano_com_descricao[
        df_procedimentos_por_ano_com_descricao['PROCEDIMENTO'] == procedimento][
        ['ANO', 'cod_municipio', 'PROCEDIMENTO', 'NIVEL', 'TX']].copy()
    df_proc[NOME_COLUNA] = df_proc['TX']
    distribuicao = NAO_NORMAL

    # teste da distribuicao - normal, log_normal
    if (len(df_proc['TX']) >= len_min):
        # testa se é distribuição normal
        if se_distribuicao_normal(df_proc['TX'], len_min):
            distribuicao = 'NORMAL ORIGINAL'
        else:
            # transformacao logaritmica
            if se_distribuicao_normal(np.log(df_proc['TX'] + 0.00000001), len_min):
                distribuicao = 'NORMAL APÓS LOG'
                df_proc[NOME_COLUNA] = np.log(df_proc['TX'] + 0.00000001)
            else:
                # transformacao - boxcox
                xt, maxlog = stats.boxcox(df_proc['TX'] + 0.00000001)
                if se_distribuicao_normal(xt, len_min) and (maxlog >= -1) and (maxlog <= 1):
                    distribuicao = 'NORMAL APÓS BOXCOX'
                    df_proc[NOME_COLUNA] = xt
    else:
        distribuicao = 'NÃO CHECADA - AMOSTRA PEQUENA'

    pop = df_proc[NOME_COLUNA]
    media = pop.mean()
    mediana = pop.median()
    std = np.std(pop)
    min_tx = pop.min()
    max_tx = pop.max()
    df_proc['DISTRIBUICAO'] = distribuicao
    df_proc['MEDIA'] = media
    df_proc['MEDIANA'] = mediana
    df_proc['MIN'] = min_tx
    df_proc['MAX'] = max_tx
    df_proc['STD'] = std

    return df_proc


def gerar_dataframes():
    df_descricao_procedimentos = sih_facade.get_df_descricao_procedimentos(ano)
    df_procedimentos_por_ano_com_descricao = sih_facade.get_df_procedimentos_por_ano_com_descricao(
        ano, df_descricao_procedimentos)
    df_descricao_procedimentos.to_csv('df_descricao_procedimentos.csv')
    df_procedimentos_por_ano_com_descricao.to_csv('df_procedimentos_por_ano_com_descricao.csv')
    return df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao


def __get_df_procedimentos_para_analise(len_min, df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao):
    df_proc_statistic_analise1 = __teste_normalidade(len_min, df_descricao_procedimentos,
                                                     df_procedimentos_por_ano_com_descricao)
    # TODO: Checar por que df_procedimentos_por_ano_com_descricao tem uma coluna a mais (Unnamed)
    df_proc_ano_completo_analise = pd.merge(df_procedimentos_por_ano_com_descricao, df_proc_statistic_analise1,
                                            on=['ANO', 'cod_municipio', 'PROCEDIMENTO', 'TX', 'NIVEL'])

    print(df_proc_ano_completo_analise.shape)
    print(df_proc_ano_completo_analise.head(2))
    return df_proc_ano_completo_analise


def get_outliers(config, df_procedimentos, df_proc_ano_completo):
    df_proc_outlier = pd.DataFrame(columns=['ANO', 'cod_municipio', 'PROCEDIMENTO', 'TX_QTD'])

    # quantidade de desvio padrão para ser considerado outlier
    qtd_std = config.get_propriedade('qtd_std_zcore')
    quantidade_vizinhos = config.get_propriedade('quantidade_vizinhos_lof')

    for procedimento in df_procedimentos.index.values:
        print('\n\n', df_procedimentos.loc[procedimento]['DESCRICAO'])

        # Obtendo dados da análise para o procedimento
        df_proc = df_proc_ano_completo[df_proc_ano_completo['PROCEDIMENTO'] == procedimento][['ANO', 'cod_municipio',
                                                                                              'PROCEDIMENTO', 'TX_QTD',
                                                                                              'DISTRIBUICAO']].copy()
        print('\n## Número de linhas: ', len(df_proc))

        # Detecção de outliers
        if (len(df_proc) > 1):
            distribuicao = df_proc['DISTRIBUICAO'].iloc[0]
            detector = SIHCompositeDetectorOutlier(df_proc, NOME_COLUNA, distribuicao, qtd_std, quantidade_vizinhos)
            outliers = detector.get_outliers()
            df_outlier = df_proc.loc[outliers, :]

            df_proc_outlier = df_proc_outlier.append(df_outlier)
        else:
            print('Quantidade de linhas insuficiente para detecção de outlier')

    df_proc_outlier['OUTLIER'] = 'S'

    return df_proc_outlier


if __name__ == '__main__':
    arquivo_configuracao = sys.argv[1]

    sih_facade = SIHFacade(arquivo_configuracao)
    ano = 2014
    # df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao = gerar_dataframes()
    df_descricao_procedimentos = pd.read_csv('df_descricao_procedimentos.csv')
    df_descricao_procedimentos = df_descricao_procedimentos.set_index('PROCEDIMENTO')
    df_procedimentos_por_ano_com_descricao = pd.read_csv('df_procedimentos_por_ano_com_descricao.csv')

    # get_df_distribuicao_nivel(len_min, df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao)
    # __get_df_procedimentos_para_analise(len_min, df_descricao_procedimentos, df_procedimentos_por_ano_com_descricao)

    config = ConfiguracoesAnalise(arquivo_configuracao)
    df_proc_ano_completo = __get_df_procedimentos_para_analise(config.get_propriedade('len_min'),
                                                               df_descricao_procedimentos,
                                                               df_procedimentos_por_ano_com_descricao)

    df_proc_outlier_analise = get_outliers(config, df_descricao_procedimentos, df_proc_ano_completo)

    df_painel_analise = pd.merge(df_proc_ano_completo, df_proc_outlier_analise,
                                 on=['ANO', 'cod_municipio', 'PROCEDIMENTO', 'TX_QTD', 'DISTRIBUICAO'], how='left')
    df_painel_analise['OUTLIER'] = df_painel_analise['OUTLIER'].fillna('N')

    print(df_painel_analise.shape)
    print(df_painel_analise.head(2))

    # coluna que identifica se o outlier é maior ou menor que a média
    df_painel_analise['OUTLIER_'] = 'N'
    df_painel_analise.loc[(df_painel_analise['OUTLIER'] == 'S') & (
            df_painel_analise['TX_QTD'] >= df_painel_analise['MEDIA']), 'OUTLIER_'] = 'S_MAIOR'
    df_painel_analise.loc[(df_painel_analise['OUTLIER'] == 'S') & (
            df_painel_analise['TX_QTD'] < df_painel_analise['MEDIA']), 'OUTLIER_'] = 'S_MENOR'

    # dado para painel
    df_painel_analise.to_csv('painel_SIH_DADOS_transformados_analise1', sep=';', index=False, decimal=',')

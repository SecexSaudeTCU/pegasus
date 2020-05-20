import numpy as np
from outliers.detector import DetectorOutlier
from scipy import stats

class ZScore(DetectorOutlier):
    def __init__(self, df, nome_coluna, qtd_std=3):
        super().__init__(df, nome_coluna)
        self.qtd_std = qtd_std

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]
        z = np.abs(stats.zscore(df_pop))
        zscore = np.where(z > self.qtd_std)
        zscore_outliers = self.df.index[zscore[0]]
        return zscore_outliers

class ZScoreModificado(ZScore):
    def __init__(self, df, nome_coluna, qtd_std=3):
        super().__init__(df, nome_coluna, qtd_std)

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]
        median_df = np.median(df_pop)
        MAD = np.median(abs(df_pop - median_df))
        z_score_modificado = 0.6745 * (df_pop - median_df) / MAD
        z_score_modificado = np.abs(z_score_modificado)

        zscore2 = np.where(z_score_modificado > self.qtd_std)
        zscore_outliers2 = self.df.index[zscore2[0]]
        return zscore_outliers2
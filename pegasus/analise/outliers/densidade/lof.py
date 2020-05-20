from outliers.detector import DetectorOutlier
from sklearn.neighbors import LocalOutlierFactor

class LOF(DetectorOutlier):

    def __init__(self, df, nome_coluna, quantidade_vizinhos=20):
        super().__init__(df, nome_coluna)
        self.__quantidade_vizinhos = quantidade_vizinhos

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]
        clf = LocalOutlierFactor(n_neighbors=self.__quantidade_vizinhos, contamination=0.1)
        outlier_lof = clf.fit_predict(df_pop.values)
        lof_outliers = self.df[outlier_lof == -1].index
        return lof_outliers
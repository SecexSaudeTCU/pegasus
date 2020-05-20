from outliers.detector import DetectorOutlier
import sklearn.ensemble
import numpy as np

class IsolationForest(DetectorOutlier):
    def __init__(self, df, nome_coluna):
        super().__init__(df, nome_coluna)

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]
        isolation_forest = sklearn.ensemble.IsolationForest(random_state=np.random.RandomState(123))
        isolation_forest.fit(df_pop.values)
        outlier_if = isolation_forest.predict(df_pop.values)
        isolation_outliers = self.df[outlier_if == -1].index
        return isolation_outliers
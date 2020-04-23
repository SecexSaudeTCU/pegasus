from outliers.detector import DetectorOutlier

class InterquartileRange(DetectorOutlier):
    def __init__(self, df, nome_coluna):
        super().__init__(self, df, nome_coluna)

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]
        Q1 = df_pop.quantile(0.25)
        Q3 = df_pop.quantile(0.75)
        IQR = Q3 - Q1
        outlier_iqr = (df_pop < (Q1 - 1.5 * IQR)) | (df_pop > (Q3 + 1.5 * IQR))
        iqr_outliers = outlier_iqr[outlier_iqr.any(axis=1)].index
        return iqr_outliers
from outliers.estatistica.zscore import ZScore, ZScoreModificado
from outliers.estatistica.iqr import InterquartileRange
from outliers.densidade.lof import LOF
from outliers.proximidade.isolation_forest import IsolationForest
from outliers.detector import DetectorOutlier

AP_S_LOG = 'NORMAL APÓS LOG'

NORMAL_ORIGINAL = 'NORMAL ORIGINAL'

AP_S_BOXCOX = 'NORMAL APÓS BOXCOX'

#TODO: Implemenar também CompositeSIADetector e checar o que existe em comum em termos de lógica

class SIHCompositeDetectorOutlier(DetectorOutlier):
    def __init__(self, df, nome_coluna, distribuicao):
        super().__init__(self, df, nome_coluna)
        self.distribuicao = distribuicao

    def get_outliers(self):
        df_pop = self.df[[self.nome_coluna]]

        print(df_pop.describe())

        if (self.distribuicao in (NORMAL_ORIGINAL, AP_S_LOG, AP_S_BOXCOX)):
            # metodos estatisticos
            # Z-score
            zscore_outliers = ZScore(self.df, self.nome_coluna).get_outliers()
            # Z-score modificado
            zscore_outliers2 = ZScoreModificado(self.df, self.nome_coluna).get_outliers()
            # IQR
            iqr_outliers = InterquartileRange(self.df, self.nome_coluna).get_outliers()

        # baseado em proximidade
        # Isolation Forest
        isolation_outliers = IsolationForest(self.df, self.nome_coluna).get_outliers()

        # baseado em densidade
        # LocalOutlierFactor
        lof_outliers = LOF(self.df, self.nome_coluna).get_outliers()

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

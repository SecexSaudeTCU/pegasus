from abc import ABC,abstractmethod

class DetectorOutlier(ABC):
    def __init__(self, df, nome_coluna):
        self.df = df
        self.nome_coluna = nome_coluna

    @abstractmethod
    def get_outliers(self):
        pass

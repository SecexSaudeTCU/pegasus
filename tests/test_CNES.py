"""
Módulo de testes unitários do banco e dos sub-bancos de dados do CNES
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')[:-14])
print(os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')[:-14])

import unittest

import pandas as pd

from pegasus.pegasus.dados.transform.extract.download_CNES import (download_CNESXXaamm,
                                                           download_table_dbf,
                                                           download_table_cnv)


class TestCNESDownload(unittest.TestCase):

    def test_dbc_CNES(self):
        df = download_CNESXXaamm('ST', 'SP', '19', '12')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(df.shape[0], 0)
        self.assertGreater(df.shape[1], 0)

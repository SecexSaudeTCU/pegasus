"""
Cria o diretório datasus_content no home directory do computador de execução deste módulo.

Código elaborado por Flávio Coelho (https://github.com/fccoelho/PySUS).
"""

import os
from pathlib import Path


if not os.path.exists(os.path.join(Path.home(), 'datasus_content')):
    os.mkdir(os.path.join(Path.home(), 'datasus_content'))
CACHEPATH = os.path.join(Path.home(), 'datasus_content')


def cache_contents():
    """
    List the files currently cached in ~/datasus_content
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]

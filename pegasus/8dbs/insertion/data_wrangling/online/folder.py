import os

"""
Cria o diretório Datasus_content no diretório root do drive D do computador Avell se não existente.

Código elaborado por Flávio Coelho (https://github.com/fccoelho/PySUS).

"""


if not os.path.exists(os.path.join('D:/', 'datasus_content')):
    os.mkdir(os.path.join('D:/', 'datasus_content'))
CACHEPATH = os.path.join('D:/', 'datasus_content')


def cache_contents():
    """
    List the files currently cached in ~/datasus_content
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]

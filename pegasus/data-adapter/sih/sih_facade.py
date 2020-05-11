from sih.dao_sih import DaoSIH


def get_df_descricao_procedimento(repositorio):
    df = repositorio.get_df_descricao_procedimentos()
    # TODO Tratar
    return df

def get_df_coordenadas(repositorio):
    df = repositorio.get_df_coordenadas()
    # TODO Tratar
    return df


def get_df_procedimentos_realizados_por_municipio_mes(repositorio):
    df = repositorio.get_df_procedimentos_realizados_por_municipio()
    # TODO Tratar
    return df

from sih.dao_sih import DaoSIH


def get_df_descricao_procedimento(repositorio):
    df = repositorio.get_df_descricao_procedimento()
    # TODO Tratar
    return df


def get_df_populacao(repositorio):
    df = repositorio.get_df_populacao()
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


if __name__ == '__main__':
    repositorio = DaoSIH()

    df = repositorio.get_df_coordenadas()
    print(df.head())

    df = repositorio.get_df_procedimentos_realizados_por_municipio(2018)
    print(df.head())

    df = repositorio.get_df_descricao_procedimento()
    print(df.head())

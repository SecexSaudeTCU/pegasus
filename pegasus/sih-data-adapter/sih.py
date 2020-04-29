from repositorio import RepositorioSIH


def get_df_estabelecimento_regiao_saude(repositorio):
    df = repositorio.get_df_estabelecimento_regiao_saude()
    # TODO Tratar
    return df


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
    repositorio = RepositorioSIH()
    df = repositorio.get_df_coordenadas()
    print(df.head())
    df = repositorio.get_df_procedimentos_realizados_por_municipio(2018)
    print(df.head())

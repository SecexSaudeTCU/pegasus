from scipy import stats


def se_distribuicao_normal(tx_values, len_min=48):
    """
    Verifica se uma dada distribuição de valores é normal.
    :param tx_values: Os valores contidos na distribuição.
    :param len_min: O tamanho mínimo para que seja possível checar a normalidade da amostra.
    :return: True, caso a distribuição seja normal, de acordo com os testes Anderson-Darling e Shapiro-Wilk, realizando
        este último apenas se o tamanho da amostra for menor que 5000, e False, caso o tamanho da distribuição seja
        menor que len_min, ou caso um dos dois testes citados (quando for possível empregar os dois testes) indique não
        normalidade.
    """
    len_amostra = len(tx_values)
    retorno = False

    if (len_amostra < len_min):
        print('Amostra muito pequena: tamanho = ', len_amostra)
    else:
        result = stats.anderson(tx_values)
        if result.statistic < result.critical_values[2]:
            # If the returned statistic is larger than these critical values then for the corresponding significance level,
            # the null hypothesis that the data come from the chosen distribution can be rejected.
            retorno = True  # normal

        if (retorno and len_amostra < 5000):
            stat, p_valor = stats.shapiro(tx_values)
            # Rejeitar H0 ao nível de significância α se Wcalculado < Wα
            retorno = retorno and p_valor > 0.05

    return retorno

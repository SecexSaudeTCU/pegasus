from sih.sih_facade import SIHFacade
import matplotlib.pyplot as plt
import pandas as pd


def exibir_grafico_qtd_linhas_por_nivel(ano, habitantes_tx, arquivo_configuracao):
    df_nivel = __get_df_nivel(ano, arquivo_configuracao, habitantes_tx)
    labels = df_nivel.NIVEL
    cores = ["#4e8b6b", "#69cacd", "#66bbe2", "#767ddd"]

    total = df_nivel.ANO.sum()
    plt.figure(figsize=(10, 5))
    plt.pie(df_nivel.ANO, autopct=lambda p: '{:.0f}'.format(p * total / 100) + '\n%.2f' % (p) + '%', startangle=90,
            colors=cores, pctdistance=1.2)

    plt.axis('equal')
    plt.legend(labels)
    plt.title('Quantidade de linhas x Nível\n\n')

    plt.show()

def exibir_grafico_barras_qtd_linhas_por_nivel(ano, habitantes_tx, arquivo_configuracao):
    plt.figure(figsize=(10, 5))
    df_nivel_join = __get_df_info_niveis(ano, habitantes_tx, arquivo_configuracao)
    plt.bar(df_nivel_join.NIVEL, df_nivel_join.ANO)
    plt.title('Quantidade de linhas x Nível\n\n')

    plt.show()

def __get_df_nivel(ano, arquivo_configuracao, habitantes_tx):
    fachada = SIHFacade(arquivo_configuracao)
    df_nivel = fachada.get_df_nivel(ano, habitantes_tx)
    return df_nivel


def exibir_grafico_qtd_tipos_servico_por_nivel(ano, habitantes_tx, arquivo_configuracao):
    df_nivel_proc = __get_df_nivel_procedimento(ano, arquivo_configuracao, habitantes_tx)

    cores = cores = ["#4e8b6b", "#69cacd", "#66bbe2", "#767ddd"]
    total = df_nivel_proc.PROCEDIMENTO.sum()
    plt.figure(figsize=(10, 5))
    plt.pie(df_nivel_proc.PROCEDIMENTO, autopct=lambda p: '{:.0f}'.format(p * total / 100), startangle=90, colors=cores,
            pctdistance=1.1)
    plt.axis('equal')

    plt.title('Quantidade de diferentes serviços x Nível\n\n')

    plt.show()

def exibir_grafico_barras_qtd_tipos_servico_por_nivel(ano, habitantes_tx, arquivo_configuracao):
    plt.figure(figsize=(10, 5))
    df_nivel_join = __get_df_info_niveis(ano, habitantes_tx, arquivo_configuracao)
    plt.bar(df_nivel_join.NIVEL, df_nivel_join.PROCEDIMENTO)
    plt.title('Quantidade de diferentes serviços x Nível\n\n')

    plt.show()

def exibir_grafico_consolidado_niveis(ano, habitantes_tx, arquivo_configuracao):
    df_nivel_join = __get_df_info_niveis(ano, habitantes_tx, arquivo_configuracao)

    fig = plt.figure(figsize=(10, 5))
    ax1 = fig.add_subplot(111)
    cor = "#4e8b6b"
    ax1.set_ylabel('Quantidade de linhas analisadas x Nível', color=cor)
    ax1.set_ylim(top=1200000)
    ax1.tick_params(axis='y', labelcolor=cor)
    rect1 = ax1.bar(df_nivel_join.NIVEL, df_nivel_join.ANO, color=cor)

    ax2 = ax1.twinx()  # cria o segundo eixo
    cor = "blue"
    ax2.set_ylabel('Quantidade de diferentes serviços x Nível', color=cor)  # cria o rótula do segundo eixo
    rect2 = ax2.plot(df_nivel_join.NIVEL, df_nivel_join.PROCEDIMENTO, color=cor)
    ax2.tick_params(axis='y', labelcolor=cor)
    ax2.set_xticks(df_nivel_join.NIVEL)

    #autolabel(rect1, rect2, ax1, ax2)
    fig.tight_layout()
    plt.show()

def __autolabel(rect1, rect2, ax, ax2):
    valores = rect2[0].get_data()[1]
    i=0
    for rect in rect1:
        height = rect.get_height()
        h = height + 50000
        cor = "#4e8b6b"
        ax.text(s=str(height)+'    ',
                    x=(rect.get_x() + rect.get_width() / 2),
                    y=h ,
                    color = cor, horizontalalignment='right' )
        cor = "blue"
        ax.text(s='       '+str(valores[i]),
                    x=(rect.get_x() + rect.get_width() / 2),
                    y=h ,
                    color = cor, horizontalalignment='left' )
        i=i+1

def __get_df_info_niveis(ano, habitantes_tx, arquivo_configuracao):
    df_nivel_join = pd.merge(__get_df_nivel(ano, arquivo_configuracao, habitantes_tx),
                             __get_df_nivel_procedimento(ano, arquivo_configuracao, habitantes_tx), on=['NIVEL'])
    print(df_nivel_join)
    return df_nivel_join

def __get_df_nivel_procedimento(ano, arquivo_configuracao, habitantes_tx):
    fachada = SIHFacade(arquivo_configuracao)
    df_nivel_proc = fachada.get_df_nivel_procedimento(ano, habitantes_tx)
    return df_nivel_proc


if __name__ == '__main__':
    arquivo_configuracao = input('Arquivo de configuração de acesso a dados:')
    habitantes_tx = 100
    # exibir_grafico_qtd_linhas_por_nivel(2014, habitantes_tx, arquivo_configuracao)
    #exibir_grafico_quantidade_tipos_servico_por_nivel(2014, habitantes_tx, arquivo_configuracao)
    #get_df_info_niveis(2014, habitantes_tx, arquivo_configuracao)
    # exibir_grafico_barras_qtd_linhas_por_nivel(
    #     2014, habitantes_tx, arquivo_configuracao
    # )
    #exibir_grafico_consolidado_niveis(2014, habitantes_tx, arquivo_configuracao)
    exibir_grafico_barras_qtd_tipos_servico_por_nivel(2014, habitantes_tx, arquivo_configuracao)

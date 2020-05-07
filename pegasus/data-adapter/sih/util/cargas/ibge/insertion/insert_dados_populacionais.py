from util.postgres.conexao import get_conexao_banco
from sih.util.cargas.ibge.extract.download import get_dados_populacionais

def criar_esquema():
    # Conecta ao banco de dados mãe "DB_NAME" do SGBD PostgreSQL usando o módulo python "psycopg2"
    conexao = get_conexao_banco('C:\\Users\\moniq\\Documents\\TCU\\SecexSaude\\pegasus\\pegasus\\data-adapter\\util\\postgres\\config.yml')
    # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
    cursor = conexao.cursor()
    # Inicializa o schema denominado "ibge" no banco de dados mãe "DB_NAME"
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS ibge;')
    conexao.commit()

    cursor.execute(f'''/*Tabela de população por municipios*/
                        CREATE TABLE IF NOT EXISTS ibge.populacao("ID"                  VARCHAR(6),
                                                                  "MUNNOME"	            VARCHAR(66),
                                                                  "MUNCODDV"            VARCHAR(7),
                                                                  "POPULACAO"           INTEGER);
        ''')
    conexao.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conexao.close()

def inserir_dados():
    #Recupera o dataframe que contém as populações de cada município
    df = get_dados_populacionais()
    df.columns = ['ID','MUNNOME','POPULACAO']
    print(df.head())

    df['MUNCODDV'] = df['ID'].str[-1]
    print(df.head())

    # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
    # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
    if len(df.loc[0, 'ID']) == 7:
        df['ID'].replace(regex='.$', value='', inplace=True)

    print(df.head())

if __name__ == '__main__':
    #criar_esquema()
    inserir_dados()


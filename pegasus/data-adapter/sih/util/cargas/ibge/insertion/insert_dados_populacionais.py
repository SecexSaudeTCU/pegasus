from util.postgres.conexao import ConfiguracoesConexaoPostgresSQL
from sih.util.cargas.ibge.extract.download import get_dados_populacionais
from sqlalchemy import create_engine

def criar_esquema(config, child_db):

    conexao = config.get_conexao()

    # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
    cursor = conexao.cursor()
    # Inicializa o schema denominado "ibge" no banco de dados mãe "DB_NAME"
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {child_db};')
    conexao.commit()

    cursor.execute(f'''/*Tabela de população por municipios*/
                        CREATE TABLE IF NOT EXISTS {child_db}.populacao("ID"                  VARCHAR(6),
                                                                        "MUNNOME"	            VARCHAR(66),
                                                                        "MUNCODDV"            VARCHAR(7),
                                                                        "POPULACAO"           INTEGER);
        ''')
    conexao.commit()
    # Encerra o cursor
    cursor.close()
    # Encerra a conexão
    conexao.close()

def inserir_dados(config, child_db):
    #Recupera o dataframe que contém as populações de cada município
    df = get_dados_populacionais()
    df.columns = ['ID','MUNNOME','POPULACAO']

    #Recupera o último dígito (verificador)
    df['MUNCODDV'] = df['ID']

    # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código...
    # do município. Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
    if len(df.loc[0, 'ID']) == 7:
        df['ID'].replace(regex='.$', value='', inplace=True)

    # URI do banco de dados mãe do SGBD PostgreSQL
    DATABASE_URI = config.get_string_conexao()
    # Cria um "engine" para o banco de dados mãe "DB_NAME" usando a função "create_engine" do SQLAlchemy
    engine = create_engine(DATABASE_URI)

    df = df[df.ID.apply(lambda x: x.isnumeric())]

    df.to_sql('populacao', con=engine, schema=child_db, if_exists='append', index=False, index_label='ID')

if __name__ == '__main__':
    # Conecta ao banco de dados mãe "DB_NAME" do SGBD PostgreSQL usando o módulo python "psycopg2"
    arquivo_configuracao = input('Caminho para o arquivo de configuração de acesso ao banco de dados:')
    config = ConfiguracoesConexaoPostgresSQL(arquivo_configuracao)

    child_db = 'ibge'
    criar_esquema(config, child_db)
    inserir_dados(config, child_db)


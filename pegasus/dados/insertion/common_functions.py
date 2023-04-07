from pandas import DataFrame
from .exception_handler import Exception_handler as handler
import psycopg2
import os 

class Insertions():    
    def __init__(self, df : DataFrame, base : str, state : str, year : str, month : str,
                    child_db : str, main_table : str):
        self.df = df
        self.base = base
        self.state = state
        self.year = year
        self.month  = month
        self.child_db = child_db
        self.main_table = main_table

    def insert_from_csv(self, connection_data):
        self.df.to_csv(self.base + self.state + self.year + self.month + '.csv', sep=',', 
                                        header=False, index=False, encoding='utf-8', escapechar=' ')
        # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do cnes_xx
        
        f = open(self.base + self.state + self.year + self.month + '.csv', 'r')
        # Conecta ao banco de dados mãe "connection_data[0]" do SGBD PostgreSQL usando o módulo python "psycopg2"
        conn = psycopg2.connect(dbname=connection_data[0],
                                host=connection_data[1],
                                port=connection_data[2],
                                user=connection_data[3],
                                password=connection_data[4])
        # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
        cursor = conn.cursor()
        # Faz a inserção dos dados armazenados em "f" na tabela "main_table" do banco de dados "child_db"
        # usando o método "copy_expert" do "psycopg2"
        
        try:
            cursor.copy_expert(f'''COPY {self.child_db}.{self.main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
        
        except psycopg2.errors.StringDataRightTruncation as exception_text:
            handler.StringDataRightTruncationHandler(exception_text,conn, self.child_db, self.main_table )
            cursor.copy_expert(f'''COPY {self.child_db}.{self.main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
        
        conn.commit()
        # Encerra o cursor
        cursor.close()
        # Encerra a conexão
        conn.close()
        # Encerra o file handler
        f.close()
        # Remoção do arquivo "csv"
        os.remove(self.base + self.state + self.year + self.month + '.csv')
        print(f'Terminou de inserir os dados do arquivo {self.base}{self.state}{self.year}{self.month} na tabela {self.main_table} do banco de dados {self.child_db}.')
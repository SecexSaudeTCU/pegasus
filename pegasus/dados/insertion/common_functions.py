from email.mime import base
from pandas import DataFrame 
from .exception_handler import Exception_handler as handler
import psycopg2
import os 
import time
from datetime import datetime
import sqlalchemy
class Insertions():    
    def __init__(self, df : DataFrame, base : str, state : str, year : str, month : str,
                    child_db : str, connection_data : list, device):
        
        self.df = df
        self.connection_data = connection_data
        self.base = base
        self.state = state
        self.year = year
        self.month  = month
        self.child_db = child_db
        self.device = device
        

    def insert_from_csv(self, main_table : str) -> None:

        
        #self.df.to_csv(self.base + self.state + self.year + self.month + '.csv', sep=',', 
        #                                header=False, index=False, encoding='latin1',escapechar=' ')
        
        # Leitura do arquivo "csv" contendo os dados do arquivo principal de dados do cnes_xx
        
        #f = open(self.base + self.state + self.year + self.month + '.csv', 'r')
        
        # Conecta ao banco de dados mãe "connection_data[0]" do SGBD PostgreSQL usando o módulo python "psycopg2"
        conn = psycopg2.connect(dbname=self.connection_data[0],
                                host=self.connection_data[1],
                                port=self.connection_data[2],
                                user=self.connection_data[3],
                                password=self.connection_data[4])
                                
        # Criação de um cursor da conexão tipo "psycopg2" referenciado à variável "cursor"
        cursor = conn.cursor()
        # Faz a inserção dos dados armazenados em "f" na tabela "main_table" do banco de dados "child_db"
        # usando o método "copy_expert" do "psycopg2"
        
        try:
            #cursor.copy_expert(f'''COPY {self.child_db}.{main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
            conn.commit()
            self.df.to_sql(main_table, con=self.device, schema=self.child_db, 
                        if_exists='append', index=False)
        
        except sqlalchemy.exc.DataError as exception_text:
            print(str(exception_text))
            print("**"*25)
            print('UEBAAAAAAA')
            print(exception_text.params)
            print("**"*25)
            handler.StringDataRightTruncationHandler(exception_text, cursor, conn, self.child_db, main_table, self.df)
            
            self.df.to_sql(main_table, con=self.device, schema=self.child_db, 
                        if_exists='append', index=False)            
            #cursor.copy_expert(f'''COPY {self.child_db}.{main_table} FROM STDIN WITH CSV DELIMITER AS ',';''', f)
            conn.commit()
        
        except Exception as other_exception:
            
            #if db_name == 'CNES':
            print("**"*25)
            print(type(other_exception))
            print("**"*25)
            #else:
            '''  print(f'Tentando a inserção do arquivo {self.base}{self.state}{self.year}{self.month} por método alternativo (pandas)...')
            self.df.to_sql(main_table, con=self.device, schema=self.child_db, 
                                    if_exists='append', index=False)
             '''

        cursor.close()
        # Encerra a conexão
        conn.close()
        # Encerra o file handler
        f.close()
        # Remoção do arquivo "csv"
        os.remove(self.base + self.state + self.year + self.month + '.csv')
        print(f'Terminou de inserir os dados do arquivo {self.base}{self.state}{self.year}{self.month} na tabela {main_table} do banco de dados {self.child_db}.')


    def atualiza_tabela_arquivos(self, file_name : str, directory : str, date_ftp : str, start : time.time) -> None:

        file_data = DataFrame(data=[[file_name, directory, date_ftp, datetime.today(), int(self.df.shape[0])]],
                             columns= ['NOME', 'DIRETORIO', 'DATA_INSERCAO_FTP', 'DATA_HORA_CARGA', 'QTD_REGISTROS'],
                             index=None
                             )
        # Inserção de informações do arquivo principal de dados no banco de dados "child_db"
        file_data.to_sql('arquivos', con=self.device, schema=self.child_db, if_exists='append', index=False)
        print(f'Terminou de inserir os metadados do arquivo {self.base}{self.state}{self.year} na tabela arquivos do banco de dados {self.child_db}.')
        end = time.time()
        print(f'Demorou {round((end - start)/60, 1)} minutos para essas duas inserções no {self.connection_data[0]}/PostgreSQL pelo pandas!')
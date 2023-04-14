import re
class Exception_handler():
    def StringDataRightTruncationHandler(exception : Exception, cursor, conn, child_db : str, main_table : str, df):
        conn.reset()       
        cursor = conn.cursor()
        
        column_error = re.search("column [^:]*:",str(exception)).group()[7:-1]
        conteudo = re.search('"[^"]*"',str(exception)).group()
        lengh_expected = len(conteudo) 

        print(f"""ERRO DE TRUNCAGEM.. alterando tamanho da coluna {column_error} para {lengh_expected}""")
        sql_update = f"""ALTER TABLE {child_db}.{main_table} ALTER COLUMN "{column_error}" TYPE VARCHAR({lengh_expected})"""
        
        print(sql_update)
        cursor.execute(sql_update.upper())
        conn.commit()
        
        

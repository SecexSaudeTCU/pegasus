import re
class Exception_handler():
    def StringDataRightTruncationHandler(conn, child_db : str, main_table : str):
        cursor.close()
        conn.reset()       
        cursor = conn.cursor()
        
        column_error = re.search("column [A-Z]*[a-z]*:",str(e)).group()[7:-1]
        conteudo = re.search('"[A-Z a-z_\*\+\-]*"',str(e)).group()
        lengh_expected = len(conteudo) 

        print(f"""ERRO DE TRUNCAGEM.. alterando tamanho da coluna {column_error} para {lengh_expected}""")
        sql_update = f"""ALTER TABLE {child_db}.{main_table} ALTER COLUMN "{column_error}" TYPE VARCHAR({lengh_expected})"""
        
        print(sql_update)
        cursor.execute(sql_update.upper())
        conn.commit()
        
        

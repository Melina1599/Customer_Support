import pandas as pd
import psycopg2
from io import StringIO
import os
import chardet

def read_csv_with_detection_content(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        detected_encoding = result['encoding']
        
        if detected_encoding is None:
            detected_encoding = 'utf-8'
    content = ""

    try:
        content = raw_data.decode(detected_encoding, errors='ignore')
    except:
        content = raw_data.decode('windows-1252', errors='ignore')

    content = content.replace('\x00', '')
    content = content.replace('«', '')

    return content

def execute_sql_file(conn, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read() 
    
    sql_statements = [s.strip() for s in sql_script.split(';') if s.strip()]

    cur = conn.cursor()
    try:
        for statement in sql_statements:
            cur.execute(statement)
        
        conn.commit()
        print(f"✅ Todas las sentencias en '{file_path}' ejecutadas exitosamente.")
        return True
    except Exception as e:
        print(f"❌ Error al ejecutar una sentencia en '{file_path}': {e}")
        conn.rollback()
        return False
    finally:
        cur.close()

def load_master_to_postgres(db_name, user, password, host=None, port=None):
    file_path = os.path.join('data', 'processed_data', 'master_support_it_2025.csv')
    
    if not os.path.exists(file_path):
        print(f"Error: El archivo maestro no se encontró en {file_path}")
        return
    print(f"Cargando datos desde: {file_path}")
    csv_content = read_csv_with_detection_content(file_path)
    file_like_object = StringIO(csv_content)


    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
            client_encoding='UTF8' # Forzar la codificación del cliente
        )
        cur = conn.cursor()        
        cur.execute("DROP TABLE IF EXISTS stg_tickets_master;")

        df_temp = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)
        columns = df_temp.columns.tolist()
        
        if not columns:
            print("❌ Error: No se encontraron columnas en el archivo CSV maestro. La tabla no puede ser creada.")
            return # Detener la ejecución si no hay columnas


        dtype_mapping = {
            'int64': 'INTEGER',
            'object': 'TEXT',
            'float64': 'NUMERIC'
        }
        
        column_definitions = []
        for col_name, dtype in df_temp.dtypes.items():
            pg_dtype = dtype_mapping.get(str(dtype), 'TEXT') # Default a TEXT si no se encuentra el tipo
            column_definitions.append(f'"{col_name}" {pg_dtype}')

        create_table_sql = f"CREATE TABLE stg_tickets_master ({', '.join(column_definitions)});"        

        print(f"Creando la tabla 'stg_tickets_master'...")
        cur.execute(create_table_sql)
    

        print(f"Cargando datos a la tabla 'stg_tickets_master' mediante COPY FROM...")
        file_like_object.seek(0)
        sql_copy = f"COPY stg_tickets_master FROM STDIN WITH CSV HEADER DELIMITER ',' NULL ''"
        cur.copy_expert(sql_copy, file_like_object)

        conn.commit()
        print(f"✅ Datos cargados exitosamente a la tabla 'stg_tickets_master' en PostgreSQL.")  
        

        #crear esquema
        sql_files = ['docs/create_dims.sql', 'docs/create_fact.sql', 'docs/load_dims.sql', 'docs/load_facts.sql'] 
        
        esquema_creado = True

        for sql_file in sql_files:
            if os.path.exists(sql_file):
                if not execute_sql_file(conn, sql_file):
                    esquema_creado = False
                    print(f"Deteniendo ejecución debido a error en {sql_file}")
                    break 
            else:
                print(f"⚠️ Archivo SQL no encontrado: {sql_file}")
                esquema_creado = False
        if esquema_creado:
            print("\nEsquema Estrella creado exitosamente.")
        else:
            print("\n❌ Error: El Esquema Estrella NO fue creado completamente")        
    except Exception as e:
        print(f"❌ Error al conectar o cargar datos a PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()

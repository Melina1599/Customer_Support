# importación de librerias basicas
import pandas as pd
import os   # manipulacion de archivos y directorios
#scripts de limpieza
from cleaning_scripts.clean_multi_lang_5 import clean_multi_lang_5
from cleaning_scripts.clean_german import clean_german
from cleaning_scripts.clean_german_5 import clean_german_5
from cleaning_scripts.clean_multi_lang_4 import clean_multi_lang_4
from cleaning_scripts.clean_multi_lang_3 import clean_multi_lang_3
from cleaning_scripts.clean_default import clean_default
#idioma
import chardet
#validacin
from validate_master import validate_master_data
#conexion BD
from load_to_postgres import load_master_to_postgres


def run_pipeline():
    processed_dir = os.path.join('data', 'processed_data')
    raw_dir = os.path.join('data', 'raw_data')
    
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    dic_cleaned_df = {}
    
    cleaning_functions_map = {
        'aa_dataset-tickets-multi-lang-5-2-50-version.csv': clean_multi_lang_5,
        'dataset-tickets-german_normalized_50_5_2.csv': clean_german_5,        
        'dataset-tickets-german_normalized.csv': clean_german, 
        'dataset-tickets-multi-lang-4-20k.csv': clean_multi_lang_4,
        'dataset-tickets-multi-lang3-4k.csv': clean_multi_lang_3,
    }

    for file in os.listdir(raw_dir):
        if file.endswith(".csv"):
            filepath = os.path.join(raw_dir, file)
            print(f'\n--- Procesando: {file} ---')
            
            clean_function = cleaning_functions_map.get(file, clean_default)                        
            df_cleaned = clean_function(filepath)
            
            print(f"Filas finales: {len(df_cleaned)}")
            
            cleaned_filename = f"cleaned_{file}"
            cleaned_file_path = os.path.join(processed_dir, cleaned_filename)
            df_cleaned.to_csv(cleaned_file_path, index=False)
            print(f"Guardado exitosamente en {processed_dir}")        
            
            key_name = file.replace('.csv', '').replace('labeled_', '')
            dic_cleaned_df[key_name] = df_cleaned

    return dic_cleaned_df


def read_csv_with_detection(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        detected_encoding = result['encoding']
        print(f"   ℹ️ Detectado: {detected_encoding} para {os.path.basename(file_path)}")    
    return pd.read_csv(file_path, encoding=detected_encoding)


# gold layer (Capa Maestra)
def create_master(dic_dfs):
    #ver
    #https://colab.research.google.com/drive/1X40E3OWrb_Iwmq0hFXZJpICchkWyUKGe?usp=sharing

    print("\n--- Iniciando Unificación del Archivo Maestro ---")
       
    file_alias_map = {
        'clean_test_aa_dataset-tickets-multi-lang-5-2-50-version.csv': 'clean_multi_lang_5',
        'clean_test_dataset-tickets-german_normalized_50_5_2.csv': 'clean_german_5',
        'clean_test_dataset-tickets-german_normalized.csv': 'clean_german',
        'clean_test_dataset-tickets-multi-lang-4-20k.csv': 'clean_multi_lang_4',
        'clean_test_dataset-tickets-multi-lang3-4k.csv': 'clean_multi_lang_3',
    }
    dfs = {}

    for name_key, df in dic_dfs.items():
        lookup_key = f"clean_test_{name_key}.csv" 
                
        alias = file_alias_map.get(lookup_key, name_key)
                
        df['origin'] = alias
        dfs[alias] = df    


    # 2. Agregacion
    #answer y type
    dfs_german_list = [dfs['clean_german_5'], dfs['clean_german']]

    for df in dfs_german_list:
        df['answer'] = 'not supported by system'
        df['ticket_type'] = 'not supported by system'

    #tag_x (1 al 8)
    for df in dfs_german_list:
        df['problem_category'] = 'not supported by system'
        df['issue_classification'] = 'not supported by system'
        df['issue_subcategory'] = 'not supported by system'
        df['issue_detail'] = 'not supported by system'
        df['resolution_category'] = 'not supported by system'
        df['resolution_subcategory'] = 'not supported by system'
        df['resolution_detail'] = 'not supported by system'
        df['additional_tag'] = 'not supported by system'


    #business type
    dfs_sin_bt_list = [dfs['clean_german'], dfs['clean_german_5'], dfs['clean_multi_lang_5'], dfs['clean_multi_lang_4']]

    for df in dfs_sin_bt_list:
        df['business_type'] = 'information not available'



    # 3. Estructura
    master_column_list = [
        'subject', 'body', 'answer', 'ticket_type', 'queue', 'sector', 'specific_sector', 
        'priority', 'priority_level', 'language', 'business_type',
        'problem_category', 'issue_classification', 'issue_subcategory', 'issue_detail',
        'resolution_category', 'resolution_subcategory', 'resolution_detail', 'additional_tag', 
        'origin'
    ]

    DEFAULT_VALUE = 'information not available'

    print("--- Estandarizando columnas en todos los DataFrames ---")
    for alias, df in dfs.items():
        print(f"\nProcesando DataFrame: {alias}")
        for col in master_column_list:
            if col not in df.columns:
                df[col] = DEFAULT_VALUE
                print(f"  Agregando columna faltante '{col}' con '{DEFAULT_VALUE}'")
        present_and_ordered = [col for col in master_column_list if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in master_column_list]

        final_column_order = present_and_ordered + remaining_columns

        dfs[alias] = df[final_column_order]
        print(f"  Columnas finales para '{alias}': {dfs[alias].columns.tolist()}")

    
    # 4. VERIFICACION final
    DEFAULT_VALUE = 'information not available'

    print("--- Estandarizando columnas en todos los DataFrames ---")
    for alias, df in dfs.items():
        missing_cols = [c for c in master_column_list if c not in df.columns]
        if missing_cols:
            print(f"\n⚠️ Procesando '{alias}': Agregando {len(missing_cols)} columnas faltantes")
            for col in missing_cols:
                df[col] = DEFAULT_VALUE
        else:
            print(f"\n✓ Dataset '{alias}' ya contiene todas las columnas maestras")

        print("Ordenando ....")  
        extra_cols = [c for c in df.columns if c not in master_column_list]
        final_order = master_column_list + extra_cols

        dfs[alias] = df[final_order]
        print(f"Estandarización completada. Total columnas: {len(dfs[alias].columns)}")


    # 5. CONCATENACIÓN
    df_master = pd.concat(dfs.values(), ignore_index=True)
    print(f"Dimensiones del maestro. Filas: {df_master.shape[0]} - Columnas: {df_master.shape[1]}")


    # 6. LIMPIEZA POST-UNIFICACIÓN (Eliminar NaNs residuales)
    df_master.fillna(DEFAULT_VALUE, inplace=True)    


    # 7. EXPORTACIÓN
    output_path = os.path.join('data', 'processed_data', 'master_support_it_2025.csv')
    df_master.to_csv(output_path, index=False, encoding='utf-8-sig')
    

    print(f"\n--- Archivo Maestro creado exitosamente: {output_path} ---")
    print(f"Total registros: {len(df_master)}")
    
    return df_master


if __name__ == "__main__":
    print("\n--- A punto de ejecutar run_pipeline() ---")
    dic_cleaned_dfs = run_pipeline() 
    
    if dic_cleaned_dfs:
        print("\n--- run_pipeline() finalizado exitosamente. A punto de ejecutar create_master() ---")
        df_final = create_master(dic_cleaned_dfs)
        print("\n--- create_master() finalizado exitosamente ---")
        print("\n--- Proceso Finalizado con Éxito ---\n\n")


        #comprobacion final
        validate_master_data()

        #creacion bd
        DB_HOST = os.environ.get('DB_HOST', 'localhost') 
        DB_PORT = os.environ.get('DB_PORT', '5433')     
        DB_NAME = os.environ.get('DB_NAME', 'customer_support_db')
        DB_USER = os.environ.get('DB_USER', 'admin')    
        DB_PASS = os.environ.get('DB_PASSWORD', 'password123')


        cleaned_db_name = DB_NAME.strip() 

        load_master_to_postgres(cleaned_db_name, DB_USER, DB_PASS, DB_HOST, DB_PORT)
    else:
        print("Error: No se procesaron archivos.")        
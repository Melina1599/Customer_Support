import os
import pandas as pd
#funciones
from ..cleaning_scripts.clean_german_5 import clean_german_5
from ..cleaning_scripts.clean_german import clean_german
from ..cleaning_scripts.clean_multi_lang_3 import clean_multi_lang_3
from ..cleaning_scripts.clean_multi_lang_4 import clean_multi_lang_4
from ..cleaning_scripts.clean_multi_lang_5 import clean_multi_lang_5


def process_ticket_file(clean_func, input_filename):
    input_path = os.path.join('data', 'raw_data', input_filename)
    
    output_filename = f"clean_test_{input_filename}"

    directory = os.path.join('data', 'processed_data', 'test_data')
    if not os.path.exists(directory):
        os.makedirs(directory)
    output_path = os.path.join('data', 'processed_data', 'test_data', output_filename)
    
    print(f"Iniciando limpieza de: {input_filename}")
    df_cleaned = clean_func(input_path) 
    
    df_cleaned.to_csv(output_path, index=False)
    print(f"✅ Archivo guardado en: {output_path}")
    return output_path


if __name__ == "__main__":
    print("--- Iniciando Orquestador de Limpieza Temporal ---")
    # Mapeo de archivos de entrada a funciones de limpieza
    files_to_process = {
        'aa_dataset-tickets-multi-lang-5-2-50-version.csv': clean_multi_lang_5,
        'dataset-tickets-german_normalized_50_5_2.csv': clean_german_5,        
        'dataset-tickets-german_normalized.csv': clean_german, 
        'dataset-tickets-multi-lang-4-20k.csv': clean_multi_lang_4,
        'dataset-tickets-multi-lang3-4k.csv': clean_multi_lang_3,
    }


    processed_files_paths = []
    for filename, function in files_to_process.items():
        path = process_ticket_file(function, filename)
        processed_files_paths.append(path)

    print(f"\n✅ Proceso de limpieza completado ✅")
    print("Los 5 archivos limpios están listos en la carpeta 'data/processed_data/test'.")
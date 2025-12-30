import pandas as pd
import os


def validate_master_data():
    file_path = os.path.join('data', 'processed_data', 'master_support_it_2025.csv')
    
    if not os.path.exists(file_path):
        print(f"❌ Error: El archivo maestro no se encontró en {file_path}")
        return
    print(f"\n🚀 --- Iniciando Validación y Limpieza del Archivo Maestro ---")
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    
    print(df.info())

    print(f"📊 Total de Filas: {len(df)}")
    print(f"📋 Total de Columnas: {len(df.columns)}")
    

    # 1. Asignar "no matter" a nulos en 'subject'
    if 'subject' in df.columns:
        if df['subject'].isnull().sum() > 0:
            df['subject'] = df['subject'].fillna('no matter')
            print(f"🛠  Valores nulos en 'subject' reemplazados por 'no matter'.")

    # 2. Convertir todo a minúsculas y 3. Eliminar comas en columnas de texto
    # Seleccionamos solo las columnas de tipo objeto (texto)
    cols_texto = df.select_dtypes(include=['object']).columns
    
    for col in cols_texto:
        df[col] = df[col].astype(str).str.lower().str.replace(',', '', regex=False)    
    print(f"✅ Texto normalizado: Todo en minúsculas y sin comas (',') en columnas de texto.")


    # 🔢 4. Validación de 'priority_level' (Rango 1-5)
    if 'priority_level' in df.columns:
        df['priority_level'] = df['priority_level'].astype(int)
        print(f"✅ 'priority_level': Verificación completada.")
        
        # Validar que los valores estén entre 1 y 5    
        invalidos = df[(df['priority_level'] < 1) | (df['priority_level'] > 5) | (df['priority_level'].isnull())]        
        if not invalidos.empty:
            print(f"⚠️  Advertencia: Se detectaron {len(invalidos)} filas con prioridad fuera de rango (1-5).")
    else:
            df['priority_level'] = df['priority_level'].astype(int)
            print(f"✅ 'priority_level': Todos los valores están en el rango correcto (1-5)")


    # Validaciones de nulos restantes
    required_cols = ['subject', 'body', 'category', 'language']
    for col in required_cols:
        if col in df.columns:
            nan_count = df[col].isnull().sum()
            if nan_count > 0:
                print(f"⚠️  Advertencia: '{col}' tiene {nan_count} valores nulos.")
            else:
                print(f"✅ '{col}' verificado correctamente.")

    # Guardar los cambios si es necesario
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"\n💾 Cambios guardados en: {file_path}")


    print("\n--- 🔍 Vista Previa Rápida ---")
    print(df.head())
    print("\n✅ Proceso de validación finalizado con éxito.")


if __name__ == "__main__":
    validate_master_data()
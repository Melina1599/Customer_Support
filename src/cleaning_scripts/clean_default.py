import pandas as pd

def clean_default(df):
    """
    Función de limpieza por defecto para archivos CSV desconocidos.
    No realiza cambios, solo devuelve el DataFrame original.
    """
    print(f"Usando limpieza por defecto para {len(df)} filas. No se aplicaron cambios.")
    
    return df
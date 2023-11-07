import pandas as pd
import numpy as np


def process_excel_file(file_path, path_bd_pn):
    # Especifica el tipo de dato de las columnas al leer el archivo
    dtype_dict = {'CODIGO DANE ': str}
    df = pd.read_excel(file_path, skiprows=9, dtype=dtype_dict)
    
    #  Encontrar el índice de la fila que contiene 'FUENTE'
    fuente_row_index = df[df['ARMA MEDIO '].str.contains('TOTAL', case=False)].index[0]
            
    # Filtrar el DataFrame desde la primera fila hasta la fila 'FUENTE'
    df = df.iloc[:fuente_row_index]
    
    # Convertir la columna "MUNICIPIO" a mayúsculas
    df['MUNICIPIO '] = df['MUNICIPIO '].str.upper()
    
    # Crear una nueva columna 'DIVIPOLA_DEP' con los primeros dos caracteres de 'CODIGO DANE'
    df['DIVIPOLA_DEP'] = df['CODIGO DANE '].str[:2]
    df['DIVIPOLA_MUN'] = df['CODIGO DANE '].str[:5]
    
    print(df)

    data = pd.read_csv(path_bd_pn, encoding='ISO-8859-3', sep=';')
    
    # Convierte la columna "fecha" en formato datetime
    data['Fecha'] = pd.to_datetime(data['Fecha'], format='%d/%m/%Y')
    data['div_dept'] = data['div_dept'].astype(str)  
    data['Div_Mun'] = data['Div_Mun'].astype(str)

    # Filtra y conserva las filas donde el año no es 2023
    data = data[data['Fecha'].dt.year != 2023]
    
    print(data)
    
    column_mapping = {
        'DIVIPOLA_DEP': 'div_dept',
        'DEPARTAMENTO ': 'Departamento',
        'DIVIPOLA_MUN': 'Div_Mun',
        'MUNICIPIO ': 'Municipio',
        'FECHA HECHO ': 'Fecha',
        'ARMA MEDIO ': 'Armas',
        'GENERO ': 'Genero',
        '*AGRUPA_EDAD_PERSONA': 'Agrupa',
        'CANTIDAD ': 'Cantidad',
    }
    path_bd_pn = "./static/archivos/tab7/resultado_fusion.csv"
    
    df_renamed = df.rename(columns=column_mapping)
    result = data.merge(df_renamed, how='outer')
    result = result.drop('CODIGO DANE ', axis=1)
    result.to_csv(path_bd_pn, index=False, sep=';')
    
    print(result)
    
    pass


def process_periodo(path_periodo = './static/archivos/tab7/resultados/Periodo.csv',
                    path_proy = './static/archivos/PROY.csv',
                    path_pdet = './static/archivos/PDET_PLAN.csv'):
    data_periodo = pd.read_csv(path_periodo, sep=';')  
    data_proy = pd.read_csv(path_proy, sep=';')  
    data_pdet = pd.read_csv(path_pdet, sep=';')  
    
    data_periodo['div_mun'] = data_periodo['div_mun'].astype(str)
    data_periodo['mes'] = data_periodo['mes'].astype(str)
    data_periodo['ano'] = data_periodo['ano'].astype(str)
    data_periodo['prueba'] = data_periodo['div_mun'].str.cat([data_periodo['mes'], data_periodo['ano']], sep='-')
    
    data_periodo.sort_values(by='prueba', ascending=False, inplace=True)
    
    data_periodo['CambioCasos'] = np.where(
    (data_periodo['div_mun'] == data_periodo['div_mun'].shift(-1)) &
    (data_periodo['mes'] == data_periodo['mes'].shift(-1)) &
    (data_periodo['ano'] != data_periodo['ano'].shift(-1)),
    data_periodo['casos'] - data_periodo['casos'].shift(-1),
    'F'
    )
    
    data_periodo['CambioPor'] = np.where(
    (data_periodo['div_mun'] == data_periodo['div_mun'].shift(-1)) &
    (data_periodo['mes'] == data_periodo['mes'].shift(-1)) &
    (data_periodo['ano'] != data_periodo['ano'].shift(-1)),
    np.where(
        data_periodo['casos'].shift(-1) == 0,
        data_periodo['casos'] * 100,
        ((data_periodo['casos'] - data_periodo['casos'].shift(-1)) / data_periodo['casos'].shift(-1)) * 100
    ),
    'F'
)
    data_periodo['CambioPor'] = pd.to_numeric(data_periodo['CambioPor'], errors='coerce')
        
    def evaluate_value(value):
        if value < 0:
            return 'MENOR'
        elif value > 0 or np.isnan(value):
            return 'MAYOR'
        else:
            return 'IGUAL'
    data_periodo['Indicador'] = data_periodo['CambioPor'].apply(evaluate_value)
    
    data_periodo['CambioPor'] = data_periodo['CambioPor'].astype(str)
    data_periodo['CambioPor'].replace('nan', 'F', inplace=True)    
    
    print(data_proy)
    print(data_pdet)
    
    data_proy['div_mun'] = data_proy['div_mun'].astype(str)
    merged_df = pd.merge(data_periodo, data_proy[['div_mun', 'Proy']], left_on='div_mun', right_on='div_mun', how='left')
    merged_df.rename(columns={'Proy': 'población'}, inplace=True)
    
    data_pdet['div_mun'] = data_pdet['div_mun'].astype(str)
    merged_pdet = pd.merge(merged_df, data_pdet[['div_mun', 'PDET']], left_on='div_mun', right_on='div_mun', how='left')
    merged_pdet_cat = pd.merge(merged_pdet, data_pdet[['div_mun', 'CATEGORIA']], left_on='div_mun', right_on='div_mun', how='left')
    merged_pdet_cat['CATEGORIA'].fillna('null', inplace=True)
    merged_pdet_cat['PDET'].fillna('N/A', inplace=True)
    
    print(merged_pdet_cat.iloc[41])
    
    merged_pdet_cat.to_csv("./static/archivos/tab7/resultados/final/Periodo_process.csv", index=False, sep=';')
    
    print(data_periodo)
    pass


def process_TCP_PN(path_periodo = './static/archivos/tab7/resultados/TCP_PN_2022_2023.csv',
                    path_proy = './static/archivos/PROY.csv',
                    path_pdet = './static/archivos/PDET_PLAN.csv'):
    
    data_periodo = pd.read_csv(path_periodo, sep=';')  
    data_proy = pd.read_csv(path_proy, sep=';')  
    data_pdet = pd.read_csv(path_pdet, sep=';')  
    
    data_periodo['div_mun'] = data_periodo['div_mun'].astype(str)
    data_periodo['mes'] = data_periodo['mes'].astype(str)
    data_periodo['ano'] = data_periodo['ano'].astype(str)
    data_periodo['concat'] = data_periodo['div_mun'].str.cat([data_periodo['mes'], data_periodo['ano']], sep='-')
    
    data_periodo.sort_values(by='concat', ascending=False, inplace=True)
    
    data_periodo['CambioCasos'] = np.where(
    (data_periodo['div_mun'] == data_periodo['div_mun'].shift(-1)) &
    (data_periodo['mes'] == data_periodo['mes'].shift(-1)) &
    (data_periodo['ano'] != data_periodo['ano'].shift(-1)),
    data_periodo['casos'] - data_periodo['casos'].shift(-1),
    'F'
    )
    
    data_periodo['CambioPor'] = np.where(
    (data_periodo['div_mun'] == data_periodo['div_mun'].shift(-1)) &
    (data_periodo['mes'] == data_periodo['mes'].shift(-1)) &
    (data_periodo['ano'] != data_periodo['ano'].shift(-1)),
    np.where(
        data_periodo['casos'].shift(-1) == 0,
        data_periodo['casos'] * 100,
        ((data_periodo['casos'] - data_periodo['casos'].shift(-1)) / data_periodo['casos'].shift(-1)) * 100
    ),
    'F'
)
    data_periodo['CambioPor'] = pd.to_numeric(data_periodo['CambioPor'], errors='coerce')
        
    def evaluate_value(value):
        if value < 0:
            return 'MENOR'
        elif value > 0 or np.isnan(value):
            return 'MAYOR'
        else:
            return 'IGUAL'
    data_periodo['Indicador'] = data_periodo['CambioPor'].apply(evaluate_value)
    
    data_periodo['CambioPor'] = data_periodo['CambioPor'].astype(str)
    data_periodo['CambioPor'].replace('nan', 'F', inplace=True)    
    
    data_periodo['mun_dep'] = data_periodo['municipio'] + '-' + data_periodo['departamento']
    
    print(data_proy)
    print(data_pdet)
    
    """data_proy['div_mun'] = data_proy['div_mun'].astype(str)
    merged_df = pd.merge(data_periodo, data_proy[['div_mun', 'Proy']], left_on='div_mun', right_on='div_mun', how='left')
    merged_df.rename(columns={'Proy': 'población'}, inplace=True)"""
    
    data_pdet['div_mun'] = data_pdet['div_mun'].astype(str)
    merged_pdet = pd.merge(data_periodo, data_pdet[['div_mun', 'PDET']], left_on='div_mun', right_on='div_mun', how='left')
    merged_pdet_cat = pd.merge(merged_pdet, data_pdet[['div_mun', 'CATEGORIA']], left_on='div_mun', right_on='div_mun', how='left')
    merged_pdet_cat['CATEGORIA'].fillna('null', inplace=True)
    merged_pdet_cat['PDET'].fillna('N/A', inplace=True)
    
    print(merged_pdet_cat.iloc[40])
    
    merged_pdet_cat.to_csv("./static/archivos/tab7/resultados/final/TCP_PN_process.csv", index=False, sep=';')
    
    print(data_periodo)
    pass

def process_Variables(path_Variables = './static/archivos/tab7/resultados/Variables_PN_2022_2023.csv'):
    
    data_variables = pd.read_csv(path_Variables, sep=';')  
    
    # Convertir la columna de fechas al tipo de dato datetime si no está en ese formato
    data_variables['fecha'] = pd.to_datetime(data_variables['fecha'])

    # Calcular el número de día
    data_variables['DIA'] = data_variables['fecha'].dt.day

    # Calcular el número de mes
    data_variables['Numero_Mes'] = data_variables['fecha'].dt.month

    # Calcular el número de semana
    data_variables['Numero_Semana'] = data_variables['fecha'].dt.isocalendar().week
    
    columns_to_drop = ['div_dpto']
    data_variables.drop(columns=columns_to_drop, inplace=True)
    
    column_mapping = {
    'div_mun': 'CODIGO_DANE',
    'fecha': 'FECHA_HECHO',
    'armas': 'ARMAS_MEDIOS',
    'genero': 'Genero',
    'agrupa': 'agrupa_edad',
    'cantidad': 'CANTIDAD',
    'ano': 'AÑO',
    'mes_letra': 'MES_',
    'dia_letra': 'DIA_SEMANA',
    'mun_dep': 'Concat municipio',
    'Numero_Semana': 'Numero_semana',
    'departamento': 'DEPARTAMENTO',
    'municipio': 'MUNICIPIO'
}

    data_variables.rename(columns=column_mapping, inplace=True)
    
    column_order=['DEPARTAMENTO','MUNICIPIO','CODIGO_DANE', 'ARMAS_MEDIOS', 'FECHA_HECHO','Genero', 'agrupa_edad', 'CANTIDAD',
                'DIA','Numero_Mes','Numero_semana', 'AÑO', 'MES_', 'DIA_SEMANA', 'Concat municipio' ]
    
    data_variables = data_variables.reindex(columns=column_order)
    data_variables['MES_'] = data_variables.apply(lambda row: f"{row['Numero_Mes']}. {row['MES_']}", axis=1)

    
    print(data_variables)
    
    column_names = data_variables.columns
    print(column_names)
    
    data_variables.to_csv("./static/archivos/tab7/resultados/final/Variables_process.csv", index=False, sep=';')

    pass


import pandas as pd
import numpy as np


def process_excel_file_tab1(file_path, path_bd_pn):
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
    df_renamed = df.rename(columns=column_mapping)
    
    path_bd_pn = "./static/archivos/tab1/resultado_fusion.csv"
    
    result = data.merge(df_renamed, how='outer')
    result = result.drop('CODIGO DANE ', axis=1)
    result.to_csv(path_bd_pn, index=False, sep=';')
    
    print(result)
    
    pass

def process_CTP_POLICIA(path_CTP = './static/archivos/tab1/resultados/Tabla_1.csv'):
    
    data_CTP = pd.read_csv(path_CTP, sep=';')  
    
    data_CTP['Lugar'] = 'Colombia'
    data_CTP['Metrica'] = 'Conteo'
    data_CTP['Violencia'] = 'Homicidios'
    data_CTP['Fuente'] = 'Policía Nacional'
    
    data_CTP['CambioConteo'] = np.where(data_CTP['sum'] < data_CTP['sum'].shift(-1), 'Menor',
                            np.where(data_CTP['sum'] > data_CTP['sum'].shift(-1), 'Mayor', 'Igual'))
    
    data_CTP['CambioCasos'] = np.abs(data_CTP['sum'] - data_CTP['sum'].shift(-1))
    data_CTP['CambioCasosPor'] = np.abs((data_CTP['sum'] / data_CTP['sum'].shift(-1)) * 100 - 100)
    
    column_mapping = {
        'ano': 'Año',
        'sum': 'Valor',
        'proyeccion': 'Población',
        'tasa': 'Tasa',
        'porcentaje': 'Porcentaje',
    }
    data_CTP = data_CTP.rename(columns=column_mapping)
    
    column_order=['Lugar',	'Metrica',	'Violencia',	'Fuente',	'Año',	'Valor',	'Población',	'Tasa',	'Porcentaje', 'CambioConteo',	'CambioCasos',	'CambioCasosPor']
    
    data_CTP = data_CTP.reindex(columns=column_order)
    
    data_CTP.to_csv("./static/archivos/tab1/resultados/final/CTP_POLICIA.csv", index=False, sep=';')

    print(data_CTP)
    pass

def process_TCP_CAT_PDET(path_TCP_CAT ,
                    path_CTP ,
                    path_pdet ):
    data_TCP = pd.read_csv(path_TCP_CAT, sep=';')  
    data_pdet = pd.read_csv(path_pdet, sep=';')  
    data_CTP = pd.read_csv(path_CTP, sep=';')  
    
    data_CTP['DEPARTAMENTO'] = 'NACIONAL'
    data_CTP['MUNICIPIO'] = 'NACIONAL'
    data_CTP['CODIGO_DANE'] = '99999'
    data_CTP['mun_dpto'] = data_CTP['MUNICIPIO'] + '-' + data_CTP['DEPARTAMENTO']
    
    print(data_CTP)
    
    column_mapping = {
        'departamento': 'DEPARTAMENTO',
        'div_mpio': 'CODIGO_DANE',
        'municipio': 'MUNICIPIO',
        'casos': 'sum',
    }
    data_TCP = data_TCP.rename(columns=column_mapping)
    data_TCP['CODIGO_DANE'] = data_TCP['CODIGO_DANE'].astype(str)
    
    result = data_CTP.merge(data_TCP, how='outer')
    
    columnas_a_eliminar = ['div_dpto', 'historico', 'hist_nal']
    result = result.drop(columns=columnas_a_eliminar)
    
    result['ano'] = result['ano'].astype(str)
    result['CONCAT'] = result['CODIGO_DANE'] + '-' + result['ano']

    result.sort_values(by='CONCAT', ascending=False, inplace=True)
    
    result['CAMBIO'] = np.where(result['MUNICIPIO'].eq(result['MUNICIPIO'].shift(-1)),
                        np.where(result['tasa'].lt(result['tasa'].shift(-1)), 'Menor', 'Mayor'),
                        'NA')
    
    result['CamPORC'] = np.where(result['MUNICIPIO'].eq(result['MUNICIPIO'].shift(-1)),
                        np.abs((result['tasa'] * 100 / result['tasa'].shift(-1)) - 100),
                        'NA')

    result['CAMBIOConteo'] = np.where(result['MUNICIPIO'].eq(result['MUNICIPIO'].shift(-1)),
                            np.where(result['sum'].lt(result['sum'].shift(-1)), 'Menor',
                                    np.where(result['sum'].gt(result['sum'].shift(-1)), 'Mayor',
                                            np.where(result['sum'].eq(result['sum'].shift(-1)), 'Igual', 'F'))),
                            'NA')
    
    result['CamCasos'] = np.where(result['MUNICIPIO'].eq(result['MUNICIPIO'].shift(-1)),
                            np.abs(result['sum'].sub(result['sum'].shift(-1))),
                            'NA')
    
    result['CamCasosPorc'] = np.where(result['MUNICIPIO'].eq(result['MUNICIPIO'].shift(-1)),
                            np.abs((result['sum'] * 100 / result['sum'].shift(-1)) - 100),
                            'NA')
    
    data_pdet['div_mun'] = data_pdet['div_mun'].astype(str)
    merged_pdet = pd.merge(result, data_pdet[['div_mun', 'PDET']], left_on='CODIGO_DANE', right_on='div_mun', how='left')
    merged_pdet['PDET'].fillna('N/A', inplace=True)
    merged_pdet = merged_pdet.drop(columns=['div_mun'])
    merged_pdet = pd.merge(merged_pdet, data_pdet[['div_mun', 'CATEGORIA']], left_on='CODIGO_DANE', right_on='div_mun', how='left')
    merged_pdet = merged_pdet.drop(columns=['div_mun'])

    column_mapping = {
        'ano': 'AÑO',
        'sum': 'CASOS',
        'proyeccion': 'POBLACIÓN',
        'tasa': 'TASA',
        'porcentaje': 'PORCENTAJE',
        'mun_dpto': 'Mun_Dep',
        

    }
    merged_pdet = merged_pdet.rename(columns=column_mapping)
    
    column_order=['DEPARTAMENTO',	'MUNICIPIO',	'AÑO',	'CONCAT',	'CODIGO_DANE',	'CASOS',	'POBLACIÓN',	'TASA',	'PORCENTAJE',	'Mun_Dep',	'CAMBIO',	'CamPORC',	'CAMBIOConteo',	'CamCasos',	'CamCasosPorc',	'PDET',	'CATEGORIA']
    
    merged_pdet = merged_pdet.reindex(columns=column_order)

    print(merged_pdet)
    print(merged_pdet.iloc[120])

    merged_pdet.to_csv("./static/archivos/tab1/resultados/final/TCP_CATEGORIA_PDET_A.csv", index=False, sep=';')

    pass


def process_TCP_DEPT(path_TCP_DEP):
    
    data_TCP_DEPT = pd.read_csv(path_TCP_DEP, sep=';')  
    
    columnas_a_eliminar = ['div_dpto', 'hist_dpto', 'porcentaje']
    data_TCP_DEPT = data_TCP_DEPT.drop(columns=columnas_a_eliminar)
    
    data_TCP_DEPT['CambioConteo'] = np.where(data_TCP_DEPT['departamento'].eq(data_TCP_DEPT['departamento'].shift(-1)),
                            np.where(data_TCP_DEPT['casos'].lt(data_TCP_DEPT['casos'].shift(-1)), 'Menor',
                                    np.where(data_TCP_DEPT['casos'].gt(data_TCP_DEPT['casos'].shift(-1)), 'Mayor', 'Igual')),
                            'NA')
    
    data_TCP_DEPT['CambioCasos'] = np.where(data_TCP_DEPT['departamento'].eq(data_TCP_DEPT['departamento'].shift(-1)),
                            np.abs(data_TCP_DEPT['casos'] - data_TCP_DEPT['casos'].shift(-1)),
                            'NA')
    
    data_TCP_DEPT['CambioCasosPor'] = np.where(data_TCP_DEPT['departamento'].eq(data_TCP_DEPT['departamento'].shift(-1)),
                            np.where(data_TCP_DEPT['casos'].eq(0), 0, np.abs((data_TCP_DEPT['casos'] * 100 / data_TCP_DEPT['casos'].shift(-1)) - 100)),
                            'NA')
    print(data_TCP_DEPT)
    
    data_TCP_DEPT.to_csv("./static/archivos/tab1/resultados/final/TCP_Departamento.csv", index=False, sep=';')

    pass


def process_HOMICIDIOS_Variables(path_Variables):
    
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
    'div_mpio': 'CODIGO_DANE',
    'fecha': 'FECHA_HECHO',
    'armas': 'ARMAS_MEDIOS',
    'genero': 'Genero',
    'agrupa': 'agrupa_edad',
    'cantidad': 'CANTIDAD',
    'ano': 'AÑO',
    'mes': 'MES_',
    'dia': 'DIA_SEMANA',
    'mun_dpto': 'Concat municipio',
    'Numero_Semana': 'Numero_semana',
    'departamento': 'DEPARTAMENTO',
    'municipio': 'MUNICIPIO',
}

    data_variables.rename(columns=column_mapping, inplace=True)
    
    column_order=['DEPARTAMENTO', 'MUNICIPIO' ,'CODIGO_DANE', 'ARMAS_MEDIOS', 'FECHA_HECHO','Genero', 'agrupa_edad', 'CANTIDAD',
                'DIA','Numero_Mes','Numero_semana', 'AÑO', 'MES_', 'DIA_SEMANA', 'Concat municipio' ]
    
    data_variables = data_variables.reindex(columns=column_order)
    
    data_variables['MES_'] = data_variables.apply(lambda row: f"{row['Numero_Mes']}. {row['MES_']}", axis=1)
    
    print(data_variables.iloc[5])
    
    column_names = data_variables.columns
    print(column_names)
    
    data_variables.to_csv("./static/archivos/tab1/resultados/final/HOMICIDIOS_Variables_2010_2021_POLICIA__A.csv", index=False, sep=';')

    pass


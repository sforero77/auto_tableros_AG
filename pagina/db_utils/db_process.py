import psycopg2
from io import StringIO
import os
from data_process.escribir_sql_tab_7 import *

def process_db(ruta_bd_pn_csv,ruta_bd_dane_csv,año_fin, mes_fin, dia_fin):
    #ruta_bd_pn_csv = "/home/sebas/Documentos/flask/pagina/static/archivos/resultado_fusion.csv"
    #ruta_bd_dane_csv = "/home/sebas/Documentos/flask/pagina/static/archivos/BD_DANE.csv"

    conn = psycopg2.connect(database = "flask_db", host="localhost", user = "postgres", password = "postgres", port= "5432")

    cur = conn.cursor()

    cur.execute(f''' 

    /* Código para tablero 2022 Pro */
    /* Copyright: Proyecto DELFOS */

    /* Creación de esquema */


    CREATE SCHEMA IF NOT EXISTS tablero_2023;
    set search_path to tablero_2023;

    /* Bases de datos suministro */

    drop table if exists bd_pn, bd_dane, pdet_categorias;

    create table tablero_2023.bd_pn(div_dpto int, departamento varchar(150), div_mun int, municipio varchar(150), fecha date, armas varchar(100), genero varchar(100), agrupa varchar(100), cantidad float);
    create table tablero_2023.bd_dane(div_dpto int, departamento varchar(150), div_mun int, municipio varchar(150), ano float, mes varchar(30), area_geo varchar(100), proy int);

    ''')

    with open(ruta_bd_pn_csv, 'r') as f:
        # Lee todas las líneas excepto la primera (encabezado)
        lines = f.readlines()[1:]
        # Crea un nuevo objeto StringIO para los datos restantes
        data_buffer = StringIO("".join(lines))
        
        # Realiza la operación de copia utilizando copy_from
        cur.copy_from(data_buffer, 'bd_pn', sep=';', null='', columns=('div_dpto', 'departamento', 'div_mun', 'municipio', 'fecha', 'armas', 'genero', 'agrupa', 'cantidad'))

    with open(ruta_bd_dane_csv, 'r', encoding='iso-8859-3') as f:
        # Lee todas las líneas excepto la primera (encabezado)
        lines = f.readlines()[1:]
        # Crea un nuevo objeto StringIO para los datos restantes
        data_buffer = StringIO("".join(lines))
        # Realiza la operación de copia utilizando el método copy_from de psycopg2
        cur.copy_from(data_buffer, 'bd_dane', sep=',', columns=('div_dpto', 'departamento', 'div_mun', 'municipio', 'ano', 'mes', 'area_geo', 'proy'))

        fecha_inicio = datetime.date(2022, 1, 1)
        fecha_fin = datetime.date(año_fin, mes_fin, dia_fin)
        
        ## creacion de sql escrtio
    cur.execute(write_sql(fecha_inicio, fecha_fin))
    
    query= '''
    select div_dpto, departamento, div_mun, municipio, ano, mes, area_geo, proy, casos, tasa, porcentaje, mun_dep from tcp_pn1
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab7/resultados/TCP_PN_2022_2023.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo TCP_PN_2022_2023")
    
    query= '''
    select div_dpto, departamento, div_mun, municipio, fecha, armas, genero, agrupa, cantidad, ano,mes_letra, dia_letra, mun_dep from variables_pn
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab7/resultados/Variables_PN_2022_2023.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Variables_PN_2022_2023")
    
    query= '''
    select div_dpto, departamento, div_mun, municipio, ano, mes, casos, fecha from historico
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab7/resultados/Semaforo_Bruto.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Semaforo_Bruto")
    
    query= '''
    select div_dpto, departamento, div_mun, municipio, ano, mes, casos, fecha from historico
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab7/resultados/Historico_2022_2023.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Historico_2022_2023")
    
    query= '''
    select div_dpto, departamento, div_mun, municipio, casos, mes, ano from periodo
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab7/resultados/Periodo.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Periodo")
    
    
    print("finalizo!!!")
    # Realiza el commit y cierra la conexión
    conn.commit()
    conn.close()

    return conn.status


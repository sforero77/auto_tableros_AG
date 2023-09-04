import psycopg2
from io import StringIO
import os

def process_db_tb1(ruta_bd_pn_csv,ruta_bd_dane_csv):
    #ruta_bd_pn_csv = "/home/sebas/Documentos/flask/pagina/static/archivos/resultado_fusion.csv"
    #ruta_bd_dane_csv = "/home/sebas/Documentos/flask/pagina/static/archivos/BD_DANE.csv"

    conn = psycopg2.connect(database = "flask_db", host="localhost", user = "postgres", password = "postgres", port= "5432")

    cur = conn.cursor()

    cur.execute(f''' 

/* Cödigo para actualizar el tablero 1 del HUB */
/* Copyright 2022. Proyecto DELFOS */

/* Entorno de desarrollo */

CREATE SCHEMA IF NOT EXISTS tab_1;
set search_path to tab_1, public;

/* Tablas utilizadas */

drop table if exists bd_pn, bd_dane;
create table bd_pn(div_dpto int, departamento varchar(100), div_mpio int, municipio varchar(100), fecha date, armas varchar(200), genero varchar(20), agrupa varchar(100), cantidad float);
create table bd_dane(cod_dpto int, nom_dpto varchar(100), cod_dane bigint, nom_mpio varchar(100), ano float, mes varchar(20), area_geo varchar(50), proyeccion float ); 

    ''')

    with open(ruta_bd_pn_csv, 'r') as f:
        # Lee todas las líneas excepto la primera (encabezado)
        lines = f.readlines()[1:]
        # Crea un nuevo objeto StringIO para los datos restantes
        data_buffer = StringIO("".join(lines))
        
        # Realiza la operación de copia utilizando copy_from
        cur.copy_from(data_buffer, 'bd_pn', sep=';', null='', columns=('div_dpto', 'departamento', 'div_mpio', 'municipio', 'fecha', 'armas', 'genero', 'agrupa', 'cantidad'))
                                                                    
    with open(ruta_bd_dane_csv, 'r', encoding='iso-8859-3') as f:
        # Lee todas las líneas excepto la primera (encabezado)
        lines = f.readlines()[1:]
        # Crea un nuevo objeto StringIO para los datos restantes
        data_buffer = StringIO("".join(lines))
        # Realiza la operación de copia utilizando el método copy_from de psycopg2
        cur.copy_from(data_buffer, 'bd_dane', sep=',', columns=(('cod_dpto', 'nom_dpto', 'cod_dane', 'nom_mpio', 'ano', 'mes', 'area_geo', 'proyeccion')))


    cur.execute('''
    
update bd_pn set departamento = 'BOGOTÁ D.C. (CT)' where municipio ilike 'BOGOTÁ D.C. (CT)';

/* Tabla 1. CTP_COL_POLICIA10_21 */

drop table if exists bd_pn_1, bd_dane_1, bd_pn_dane;

create temporary table bd_pn_1 as
select distinct extract(year from fecha) as ano, sum(cantidad) from bd_pn group by extract(year from fecha) order by extract(year from fecha) desc;

create temporary table bd_dane_1 as
select distinct ano, sum(proyeccion) as proyeccion from bd_dane 
	where (mes ilike '%enero%') and (area_geo ilike '%Total%') 
		group by ano order by ano desc;
		
create temporary table bd_pn_dane as
select bd1.*, bd2.proyeccion, (bd1.sum/bd2.proyeccion)*100000 as tasa, (bd1.sum/(select sum(cantidad) from bd_pn))*100 as porcentaje 
	from bd_pn_1 as bd1 left join bd_dane_1 as bd2 on (bd1.ano = bd2.ano) order by bd1.ano desc;

	/* Los cálculos de cambios en conteo y porcentaje se calculan en la hoja de cálculo, organizando de forma descendente */
	/* El archivo CTP_COL_POLICIA10_21.xlsx contiene la formulación para cambios, sólo actualizar el periodo de estudio */

/* Tabla 2. HOMIC_POL_2021ABRIL_BGTA Y Tabla 4. HOMICIDIOS_POL_2021_TCP */

drop table if exists municipios, bd_pn_2, bd_pn_dane_2, bd_hist_pn, tcp_pn_2, municipios_pn;

create temporary table municipios as
select distinct cod_dane, nom_mpio, ano, proyeccion from bd_dane 
	where area_geo ilike '%Total%'
		group by cod_dane, nom_mpio, ano, proyeccion;

create temporary table bd_pn_2 as
select div_dpto, departamento, div_mpio, municipio, extract(year from fecha) as ano, sum(cantidad) as casos from bd_pn
	group by div_dpto, departamento, div_mpio, municipio, extract(year from fecha);

create temporary table bd_pn_dane_2 as
select bd1.cod_dane, bd2.ano as ano_pn, bd1.ano as ano_dane, bd2.casos, bd1.proyeccion from municipios as bd1 
	left join bd_pn_2 as bd2 on ((bd1.cod_dane = bd2.div_mpio) and (bd1.ano = bd2.ano));

update bd_pn_dane_2 set casos = '0' where casos is null;
update bd_pn_dane_2 set ano_pn = ano_dane where ano_pn is null;

create temporary table bd_hist_pn as
select distinct div_mpio, sum(cantidad) as historico from bd_pn group by div_mpio order by div_mpio;

create temporary table tcp_pn_2 as
select bd1.*, bd2.historico, bd3.sum as hist_nal, (bd1.casos/bd1.proyeccion)*100000 as tasa, (bd1.casos/bd3.sum)*100 as porcentaje
	from bd_pn_dane_2 as bd1 left join bd_hist_pn as bd2 on (bd1.cod_dane = bd2.div_mpio)
			left join bd_pn_1 as bd3 on (bd1.ano_pn = bd3.ano)
				where bd2.historico is not null order by cod_dane, ano_pn;

create temporary table municipios_pn as
select distinct div_dpto, departamento, div_mpio, municipio from bd_pn
	group by div_dpto, departamento, div_mpio, municipio;

create temporary table bd_pn_dane_t2 as
select bd1.*, bd2.ano_pn as ano, bd2.casos, bd2.proyeccion, bd2.historico, bd2.hist_nal, 
	bd2.tasa, bd2.porcentaje, ( bd1.municipio || ' - ' || bd1.departamento) as mun_dpto from tcp_pn_2 as bd2 left join municipios_pn as bd1
		on (bd2.cod_dane = bd1.div_mpio);

	/* Los cálculos de cambios en conteo y porcentaje se calculan en la hoja de cálculo, organizando de forma descendente */
	/* El archivo HOMICIDIOS_POL_2021_TCP.xlsx contiene la formulación para cambios, sólo actualizar el periodo de estudio */

/* Tabla 3. Homicidios_CT_POLICIA_2010_2021 y Tabla 6. TCP_Departamento_2010_2021 */

drop table if exists departamentos, casos_pn, bd_pn_dane_3, hist_dpto, bd_pn_dane_t3;

create temporary table departamentos as
select distinct cod_dpto, nom_dpto, ano, sum(proyeccion) as proyeccion from bd_dane
	where mes ilike '%Enero%' and area_geo ilike '%Total%'
		group by cod_dpto, nom_dpto, ano
			order by cod_dpto, nom_dpto, ano;

create temporary table casos_pn as
select distinct div_dpto, departamento, ano, sum(cantidad) as casos from (select *, extract(year from fecha) as ano from bd_pn) as bd1
	group by div_dpto, departamento, ano
		order by div_dpto, departamento;

create temporary table bd_pn_dane_3 as
select bd1.*, bd2.proyeccion, (bd1.casos/bd2.proyeccion)*100000 as tasa
		from casos_pn as bd1 left join departamentos as bd2 
			on (bd2.cod_dpto = bd1.div_dpto) and (bd1.ano = bd2.ano)
				order by bd1.div_dpto, bd1.departamento;

create temporary table hist_dpto as
select distinct div_dpto, sum(casos) from casos_pn group by div_dpto;

create temporary table bd_pn_dane_t3 as
select bd1.*, bd2.sum as hist_dpto, (bd1.casos/bd2.sum)*100 as porcentaje
	from bd_pn_dane_3 as bd1 left join hist_dpto as bd2 on (bd1.div_dpto = bd2.div_dpto);

/* Tabla 5. HOMICIDIOS_Variables_2010_2021_ABRIL_POLICIA_ */

create temporary table bd_pn_dane_t5 as
select *, extract(year from fecha) as ano, to_char(fecha, 'TMMonth') as mes, to_char(fecha, 'TMDay') as dia, 
	(municipio || ' - ' || departamento) as mun_dpto from bd_pn;

/* --------------------------------- */

    
    ''')
    
    query= '''
    select ano, sum, proyeccion, tasa, porcentaje from bd_pn_dane
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab1/resultados/Tabla_1.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Tabla_1")
    
    query= '''
    select div_dpto, departamento, div_mpio, municipio, ano, casos, proyeccion, historico, hist_nal, tasa, porcentaje, mun_dpto from bd_pn_dane_t2
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab1/resultados/Tabla_2.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Tabla_2")
    
    query= '''
    select div_dpto, departamento, ano, casos, proyeccion, tasa, hist_dpto, porcentaje from bd_pn_dane_t3
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab1/resultados/Tabla_3.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Tabla_3")
    
    query= '''
    select div_dpto, departamento, div_mpio, municipio, fecha, armas, genero, agrupa, cantidad, ano, mes, dia, mun_dpto from bd_pn_dane_t5
    '''
    outputquery= "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query)
    
    with open("./static/archivos/tab1/resultados/Tabla_5.csv", "w") as f:
        cur.copy_expert(outputquery, f)
    print("finalizo Tabla_5")


    
    
    print("finalizo!!!")
    # Realiza el commit y cierra la conexión
    conn.commit()
    conn.close()

    return conn.status


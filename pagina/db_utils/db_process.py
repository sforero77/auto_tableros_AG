import psycopg2
from io import StringIO
import os

def process_db(ruta_bd_pn_csv,ruta_bd_dane_csv):
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


    cur.execute('''
    
/* Creación de BD Productos */


drop table if exists bd_pn_1, bd_dane_1, bd_pn_dane_1;

create temporary table bd_pn_1 as
select div_dpto, departamento, div_mun, municipio, extract(year from fecha) as ano, to_char(fecha, 'TMMonth') as mes_letra, sum(cantidad) as casos from bd_pn group by div_dpto, departamento, div_mun, municipio, ano, mes_letra;

create temporary table bd_dane_1 as
select * from bd_dane where (area_geo ilike '%Total%') and ((ano = '2022') or (ano = '2023'));

create temporary table bd_pn_dane_1 as
select bd1.*, bd2.casos from bd_dane_1 as bd1 left join bd_pn_1 as bd2 on ((bd2.div_mun = bd1.div_mun) and (bd2.ano = bd1.ano) and (bd2.mes_letra = bd1.mes));

update bd_pn_dane_1 set casos = '0' where casos is null;

select * from bd_pn_dane_1 limit 10;

/* ----------------------------- */
/* Resultado 1: TCP PN 2021 2022 */

create temporary table tcp_pn1 as
select *, cast(((casos/proy)*100000) as float) as tasa, 
	((casos/(select sum(cantidad) from bd_pn
				where (extract(year from fecha) = ano) and (to_char(fecha, 'TMMonth') = mes) )))*100 as porcentaje,
					(municipio || ' - ' || departamento) as mun_dep
						from bd_pn_dane_1 where not proy = '0';

/* Resultado 2: Variables PN 2021 2022 */

drop table if exists variables_pn;

create temporary table variables_pn as
select *, extract(year from fecha) as ano, to_char(fecha, 'TMMonth') as mes_letra, to_char(fecha, 'TMDay') as dia_letra, (municipio || ' - ' || departamento) as mun_dep from bd_pn;

/* Resultado 3: Semaforo e Histórico */

drop table if exists municipios, homi_pn, historico;

create temporary table municipios as
select distinct div_dpto, departamento, div_mun, municipio, ano, mes from bd_dane where ((ano = '2022') or (ano = '2023')) and (area_geo = 'Total')
	order by municipio, ano, mes;

create temporary table homi_pn as
select div_mun, sum(cantidad) as casos, extract(year from fecha) as ano, to_char(fecha, 'TMMonth') as mes_letra from bd_pn
	group by div_mun, ano, mes_letra
		order by div_mun, ano, mes_letra;

create temporary table historico as
select bd1.*, bd2.casos from municipios as bd1 left join homi_pn as bd2 on ((bd2.div_mun = bd1.div_mun) and (bd2.ano = bd1.ano) and (bd2.mes_letra = bd1.mes));

update historico set casos = '0' where casos is null;

alter table historico add column fecha date;

update historico set fecha = '31/01/2022' where (ano = '2022') and (mes = 'Enero');
update historico set fecha = '28/02/2022' where (ano = '2022') and (mes = 'Febrero');
update historico set fecha = '31/03/2022' where (ano = '2022') and (mes = 'Marzo');
update historico set fecha = '30/04/2022' where (ano = '2022') and (mes = 'Abril');
update historico set fecha = '31/05/2022' where (ano = '2022') and (mes = 'Mayo');
update historico set fecha = '30/06/2022' where (ano = '2022') and (mes = 'Junio');
update historico set fecha = '31/07/2022' where (ano = '2022') and (mes = 'Julio');
update historico set fecha = '31/08/2022' where (ano = '2022') and (mes = 'Agosto');
update historico set fecha = '30/09/2022' where (ano = '2022') and (mes = 'Septiembre');
update historico set fecha = '31/10/2022' where (ano = '2022') and (mes = 'Octubre');
update historico set fecha = '30/11/2022' where (ano = '2022') and (mes = 'Noviembre');
update historico set fecha = '31/12/2022' where (ano = '2022') and (mes = 'Diciembre');
update historico set fecha = '31/01/2023' where (ano = '2023') and (mes = 'Enero');
update historico set fecha = '28/02/2023' where (ano = '2023') and (mes = 'Febrero');
update historico set fecha = '31/03/2023' where (ano = '2023') and (mes = 'Marzo');
update historico set fecha = '30/04/2023' where (ano = '2023') and (mes = 'Abril');
update historico set fecha = '31/05/2023' where (ano = '2023') and (mes = 'Mayo');
update historico set fecha = '30/06/2023' where (ano = '2023') and (mes = 'Junio');
update historico set fecha = '31/07/2023' where (ano = '2023') and (mes = 'Julio');
select * from historico;

/* Resultado 4: Periodo [Bimestre, Trimestre, Semestre, Anual] */

drop table if exists mun_r4, homi_tri_2022, homi_tri_2023;

create temporary table mun_r4 as
select distinct div_dpto, departamento, div_mun, municipio from bd_dane where ((ano = '2022')) and (area_geo = 'Total')
	order by municipio;

create temporary table homi_tri_2022 as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
	where ((to_char(fecha, 'TMMonth') = 'Enero') or (to_char(fecha, 'TMMonth') = 'Febrero') or (to_char(fecha, 'TMMonth') = 'Marzo') or (to_char(fecha, 'TMMonth') = 'Abril') or (to_char(fecha, 'TMMonth') = 'Mayo') or (to_char(fecha, 'TMMonth') = 'Junio') or (to_char(fecha, 'TMMonth') = 'Julio')) and (extract(year from fecha) = '2022')
		group by div_dpto, departamento, div_mun, municipio;

create temporary table homi_tri_2023 as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
	where ((to_char(fecha, 'TMMonth') = 'Enero')or (to_char(fecha, 'TMMonth') = 'Febrero') or (to_char(fecha, 'TMMonth') = 'Marzo') or (to_char(fecha, 'TMMonth') = 'Abril') or (to_char(fecha, 'TMMonth') = 'Mayo')or (to_char(fecha, 'TMMonth') = 'Junio') or (to_char(fecha, 'TMMonth') = 'Julio')) and (extract(year from fecha) = '2023')
	group by div_dpto, departamento, div_mun, municipio;
 
/* Unión con los 1122 Municipios */

drop table if exists trim_2022, trim_2023;

create temporary table trim_2022 as
select	bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_2022 as bd2 on (bd1.div_mun = bd2.div_mun);
	
alter table trim_2022 add column mes varchar(100);
alter table trim_2022 add column ano float;

update trim_2022 set mes = 'Enero - Julio';
update trim_2022 set ano = '2022';
update trim_2022 set casos = '0' where casos is null;
	
create temporary table trim_2023 as
select	bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_2023 as bd2 on (bd1.div_mun = bd2.div_mun);

alter table trim_2023 add column mes varchar(100);
alter table trim_2023 add column ano float;

update trim_2023 set mes = 'Enero - Julio';
update trim_2023 set ano = '2023';
update trim_2023 set casos = '0' where casos is null;

insert into trim_2022(div_dpto, departamento, div_mun, municipio, casos, mes, ano) select div_dpto, departamento, div_mun, municipio, casos, mes, ano from trim_2023;

drop table if exists periodo;

create temporary table periodo as
select * from trim_2022;

    
    ''')
    
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


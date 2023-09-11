import datetime
import locale



def write_sql(fecha_inicio, fecha_fin):
    # Definir las fechas de inicio y fin que deseas para el rango de meses
    
    texto_sql='''
        
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
    '''

    # Definir el intervalo de meses
    intervalo_meses = 1  # Por defecto, actualiza mes a mes

    sentencias_sql = ""
    # Crear un archivo para escribir las sentencias SQL
    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        # Extraer el año y el mes de la fecha actual
        ano_actual = fecha_actual.year
        mes_actual = fecha_actual.month
        mes_letra = fecha_actual.strftime('%B')  # Usar '%B' para obtener el nombre del mes en español

        # Mapear nombres de meses en inglés a español
        meses_en_ingles = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        meses_en_espanol = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
        mes_letra = meses_en_espanol[meses_en_ingles.index(mes_letra)]

        # Calcular el último día del mes
        ultimo_dia_mes = (fecha_actual.replace(day=1, month=mes_actual % 12 + 1, year=ano_actual) - datetime.timedelta(days=1)).day

        # Construir la sentencia UPDATE y escribirla en el archivo
        sentencia_sql = "UPDATE historico SET fecha = '{}/{:02d}/{}' WHERE (ano = '{}' AND mes = '{}');\n".format(ultimo_dia_mes, mes_actual, ano_actual, ano_actual, mes_letra)
        
        sentencias_sql += sentencia_sql
        
        # Avanzar al siguiente mes
        mes_actual += intervalo_meses
        if mes_actual > 12:
            mes_actual = 1
            ano_actual += 1
        fecha_actual = datetime.date(ano_actual, mes_actual, 1)

    texto_sql+=sentencias_sql


    texto_sql+= f'''

    select * from historico;

    /* Resultado 4: Periodo [Bimestre, Trimestre, Semestre, Anual] */

    drop table if exists mun_r4, homi_tri_2022, homi_tri_2023;

    create temporary table mun_r4 as
    select distinct div_dpto, departamento, div_mun, municipio from bd_dane where ((ano = '2022')) and (area_geo = 'Total')
        order by municipio;

    '''

    # Extraer el mes final de fecha_fin en español
    mes_final = fecha_fin.strftime('%B')

    # Mapear nombres de meses en inglés a español
    meses_en_ingles = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    meses_en_espanol = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    mes_final_espanol = meses_en_espanol[meses_en_ingles.index(mes_final)]

    consultas_sql_2=""

    for ano in range(fecha_inicio.year, fecha_fin.year + 1):
        consulta_sql_2 = f"""
    create temporary table homi_tri_{ano} as
    select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
        where ({' or '.join([f"(to_char(fecha, 'TMMonth') = '{mes}')" for mes in meses_en_espanol[:meses_en_espanol.index(mes_final_espanol) + 1]])}) and (extract(year from fecha) = '{ano}')
        group by div_dpto, departamento, div_mun, municipio;
    """
        consultas_sql_2 += consulta_sql_2

    texto_sql+=consultas_sql_2

    texto_sql += f'''

    /* Unión con los 1122 Municipios */

    drop table if exists trim_2022, trim_2023;

        '''

    # Establecer la configuración local para obtener los meses en español
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

    # Crear un archivo para escribir las consultas SQL
    consultas_sql_3 = ""

    for year in range(fecha_inicio.year, fecha_fin.year + 1):
        # Obtener el último mes según el año en español con la primera letra en mayúscula
        mes_final = fecha_fin.strftime('%B').capitalize()
        
        # Generar consulta SQL
        consulta_sql_3 = f"""
    drop table if exists trim_{year};

    create temporary table trim_{year} as
    select bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_{year} as bd2 on (bd1.div_mun = bd2.div_mun);

    alter table trim_{year} add column mes varchar(100);
    alter table trim_{year} add column ano float;

    update trim_{year} set mes = 'Enero - {mes_final}';
    update trim_{year} set ano = '{year}';
    update trim_{year} set casos = '0' where casos is null;
    """

        # Agregar la consulta SQL al texto existente
        consultas_sql_3 += consulta_sql_3


    texto_sql += consultas_sql_3

    texto_sql += f"""

    insert into trim_2022(div_dpto, departamento, div_mun, municipio, casos, mes, ano) select div_dpto, departamento, div_mun, municipio, casos, mes, ano from trim_2023;

    drop table if exists periodo;

    create temporary table periodo as
    select * from trim_2022;

    """

    print(texto_sql)
    
    return texto_sql
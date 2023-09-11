import datetime

# Definir las fechas de inicio y fin que deseas para el rango de meses
fecha_inicio = datetime.date(2022, 1, 1)
fecha_fin = datetime.date(2024, 12, 1)

# Extraer el mes final de fecha_fin en español
mes_final = fecha_fin.strftime('%B')

# Mapear nombres de meses en inglés a español
meses_en_ingles = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
meses_en_espanol = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
mes_final_espanol = meses_en_espanol[meses_en_ingles.index(mes_final)]

# Crear un archivo para escribir las consultas SQL
with open('consultas_sql.sql', 'w') as archivo_sql:
    for ano in range(fecha_inicio.year, fecha_fin.year + 1):
        consulta_sql = f"""
create temporary table homi_tri_{ano} as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
    where ({' or '.join([f"(to_char(fecha, 'TMMonth') = '{mes}')" for mes in meses_en_espanol[:meses_en_espanol.index(mes_final_espanol) + 1]])}) and (extract(year from fecha) = '{ano}')
    group by div_dpto, departamento, div_mun, municipio;
"""
        archivo_sql.write(consulta_sql)

print("Las consultas SQL se han generado en el archivo 'consultas_sql.sql'.")

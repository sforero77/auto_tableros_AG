import datetime
import locale

# Establecer la configuración local para obtener los meses en español
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

# Definir las fechas de inicio y fin que deseas para el rango de meses
fecha_inicio = datetime.date(2022, 1, 1)
fecha_fin = datetime.date(2023, 10, 1)  # Ajustar esta fecha según tus necesidades

# Obtener el último mes según la fecha de finalización en español con la primera letra en mayúscula
mes_final = fecha_fin.strftime('%B').capitalize()

# Crear un archivo para escribir las consultas SQL
with open('consultas_sql_nuevo.sql', 'w') as archivo_sql:
    for year in range(fecha_inicio.year, fecha_fin.year + 1):
        # Generar consulta SQL
        consulta_sql = f"""
drop table if exists trim_{year};

create temporary table trim_{year} as
select bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_{year} as bd2 on (bd1.div_mun = bd2.div_mun);

alter table trim_{year} add column mes varchar(100);
alter table trim_{year} add column ano float;

update trim_{year} set mes = 'Enero - {mes_final}';
update trim_{year} set ano = '{year}';
update trim_{year} set casos = '0' where casos is null;
"""
        archivo_sql.write(consulta_sql)

print("Las consultas SQL se han generado en el archivo 'consultas_sql_nuevo.sql'.")

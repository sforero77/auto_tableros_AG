import datetime

# Definir las fechas de inicio y fin que deseas actualizar
fecha_inicio = datetime.date(2022, 1, 1)  # Cambia la fecha de inicio según tus necesidades
fecha_fin = datetime.date(2023, 7, 1)     # Cambia la fecha de fin según tus necesidades

# Definir el intervalo de meses
intervalo_meses = 1  # Por defecto, actualiza mes a mes

# Crear un archivo para escribir las sentencias SQL
with open('sentencias_update.sql', 'w') as archivo_sql:
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
        archivo_sql.write(sentencia_sql)

        # Avanzar al siguiente mes
        mes_actual += intervalo_meses
        if mes_actual > 12:
            mes_actual = 1
            ano_actual += 1
        fecha_actual = datetime.date(ano_actual, mes_actual, 1)

print("Las sentencias UPDATE se han escrito en el archivo 'sentencias_update.sql'.")


create temporary table homi_tri_2022 as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
    where ((to_char(fecha, 'TMMonth') = 'Enero') or (to_char(fecha, 'TMMonth') = 'Febrero') or (to_char(fecha, 'TMMonth') = 'Marzo') or (to_char(fecha, 'TMMonth') = 'Abril') or (to_char(fecha, 'TMMonth') = 'Mayo') or (to_char(fecha, 'TMMonth') = 'Junio') or (to_char(fecha, 'TMMonth') = 'Julio') or (to_char(fecha, 'TMMonth') = 'Agosto') or (to_char(fecha, 'TMMonth') = 'Septiembre') or (to_char(fecha, 'TMMonth') = 'Octubre') or (to_char(fecha, 'TMMonth') = 'Noviembre') or (to_char(fecha, 'TMMonth') = 'Diciembre')) and (extract(year from fecha) = '2022')
    group by div_dpto, departamento, div_mun, municipio;

create temporary table homi_tri_2023 as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
    where ((to_char(fecha, 'TMMonth') = 'Enero') or (to_char(fecha, 'TMMonth') = 'Febrero') or (to_char(fecha, 'TMMonth') = 'Marzo') or (to_char(fecha, 'TMMonth') = 'Abril') or (to_char(fecha, 'TMMonth') = 'Mayo') or (to_char(fecha, 'TMMonth') = 'Junio') or (to_char(fecha, 'TMMonth') = 'Julio') or (to_char(fecha, 'TMMonth') = 'Agosto') or (to_char(fecha, 'TMMonth') = 'Septiembre') or (to_char(fecha, 'TMMonth') = 'Octubre') or (to_char(fecha, 'TMMonth') = 'Noviembre') or (to_char(fecha, 'TMMonth') = 'Diciembre')) and (extract(year from fecha) = '2023')
    group by div_dpto, departamento, div_mun, municipio;

create temporary table homi_tri_2024 as
select div_dpto, departamento, div_mun, municipio, sum(cantidad) as casos from bd_pn 
    where ((to_char(fecha, 'TMMonth') = 'Enero') or (to_char(fecha, 'TMMonth') = 'Febrero') or (to_char(fecha, 'TMMonth') = 'Marzo') or (to_char(fecha, 'TMMonth') = 'Abril') or (to_char(fecha, 'TMMonth') = 'Mayo') or (to_char(fecha, 'TMMonth') = 'Junio') or (to_char(fecha, 'TMMonth') = 'Julio') or (to_char(fecha, 'TMMonth') = 'Agosto') or (to_char(fecha, 'TMMonth') = 'Septiembre') or (to_char(fecha, 'TMMonth') = 'Octubre') or (to_char(fecha, 'TMMonth') = 'Noviembre') or (to_char(fecha, 'TMMonth') = 'Diciembre')) and (extract(year from fecha) = '2024')
    group by div_dpto, departamento, div_mun, municipio;


drop table if exists trim_2022;

create temporary table trim_2022 as
select bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_2022 as bd2 on (bd1.div_mun = bd2.div_mun);

alter table trim_2022 add column mes varchar(100);
alter table trim_2022 add column ano float;

update trim_2022 set mes = 'Enero - Diciembre';
update trim_2022 set ano = '2022';
update trim_2022 set casos = '0' where casos is null;

drop table if exists trim_2023;

create temporary table trim_2023 as
select bd1.*, bd2.casos from mun_r4 as bd1 left join homi_tri_2023 as bd2 on (bd1.div_mun = bd2.div_mun);

alter table trim_2023 add column mes varchar(100);
alter table trim_2023 add column ano float;

update trim_2023 set mes = 'Enero - Agosto';
update trim_2023 set ano = '2023';
update trim_2023 set casos = '0' where casos is null;

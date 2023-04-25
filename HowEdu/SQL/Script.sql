-- CREATE TABLE BILLBOARD
CREATE TABLE PUBLIC."Billboard" (
	"date" DATE NULL
	,"rank" INT NULL
	,song VARCHAR(300) NULL
	,artist VARCHAR(300) NULL
	,"last-week" FLOAT NULL
	,"peak-rank" INT NULL
	,"weeks-on-board" INT NULL
	);

SELECT *
FROM PUBLIC."Billboard" limit 100;

SELECT count(*) AS quantidade
FROM PUBLIC."Billboard" limit 100;

SELECT t1."date"
	,t1."rank"
	,t1.song
	,t1.artist
	,t1."last-week"
	,t1."peak-rank"
	,t1."weeks-on-board"
FROM PUBLIC."Billboard" AS t1 limit 100;

-- FILTRAR POR SONG E ARTISTA ONDE ARTISTA = 'Chuck Berry'

select t1.song
	,t1.artist
FROM PUBLIC."Billboard" AS t1
where t1.artist = 'Chuck Berry';


-- CONTAR QUANTAS VEZES CADA MUSICA DO CHUCK BERRY APARECEU NA BILLBOARD.

select t1.artist,
	   t1.song,
	count(*) as qtd_musica
FROM PUBLIC."Billboard" AS t1
where t1.artist = 'Chuck Berry'
group by t1.artist, t1.song;

-- ORDENADO PELA MAIOR QUANTIDADE DE VEZES "#song"

select t1.artist,
	   t1.song,
	   count(*) as "#song"
FROM PUBLIC."Billboard" AS t1
where t1.artist = 'Chuck Berry'
group by t1.artist, t1.song
order by "#song" desc;

-- ORDENANDO PELA MAIOR QUANTIDADE DE VEZES DO CHUCK BERRY E DO FRANKIE VAUGHAN

select t1.artist,
	   t1.song,
	   count(*) as "#song"
FROM PUBLIC."Billboard" AS t1
where t1.artist = 'Chuck Berry'
or t1.artist = 'Frankie Vaughan'
group by t1.artist, t1.song
order by "#song" desc;

-- OUTRA VERSÃO

select t1.artist, 
	   t1.song,
	   count(*) as "#song"
from public."Billboard" as t1
where t1.artist in('Chuck Berry', 'Frankie Vaughan')
group by t1.artist, t1.song
order by "#song" desc;


-- CTE e window functions

SELECT t1."date"
	,t1."rank"
	,t1.song
	,t1.artist
	,t1."last-week"
	,t1."peak-rank"
	,t1."weeks-on-board"
FROM PUBLIC."Billboard" AS t1 limit 100;

-- selecione os artistas e musicas e ordene por artista e musica.

select t1.artist, t1.song
from public."Billboard" as t1
order by t1.artist, t1.song;

-- selecione as musicas de cada artista usando distinct

select distinct t1.artist, t1.song
from public."Billboard" as t1
order by t1.artist, t1.song;

-- selecione quantas vezes os artistas aparecem

select t1.artist, count(*) as qtd_artist
from public."Billboard" as t1
group by t1.artist
order by t1.artist

-- selecione quantas vezes as musicas aparecem

select t1.song, count(*) as qtd_song
from public."Billboard" as t1
group by t1.song
order by t1.song

-- CTE -- conte a quantidade de artistas e a quantidade de musicas 

with cte_artist as (
	select t1.artist, count(*) as qtd_artist
	from public."Billboard" as t1
	group by t1.artist
	order by t1.artist
), 
cte_song as(
 	select t1.song, count(*) as qtd_song
 	from public."Billboard" as t1
 	group by t1.song
 	order by t1.song
 )
 
 select distinct t1.artist, t2.qtd_artist, t1.song, t3.qtd_song
 from public."Billboard" as t1
 left join cte_artist as t2 on(t1.artist = t2.artist)
 left join cte_song as t3 on(t1.song = t3.song)
 order by t1.artist, t1.song
 
 -- window function 
 -- criar uma coluna para enumerar os resultados usando row_number
 
 with cte_billboard as(
 	select distinct t1.artist, t1.song
	from public."Billboard" as t1
	order by t1.artist, t1.song
)

select * , row_number() over(order by artist, song) as "row_number"
		 , row_number() over(partition by artist order by artist, song) as "row_number_artist"
from cte_billboard;

-- contar a primeira vez que o artista aparece 

with cte_billboard as (
	select distinct t1.artist, t1.song, 
	row_number() over(order by artist, song) as "row_number",
	row_number() over(partition by artist order by artist, song) as "row_number_artist"
from public."Billboard" as t1
order by t1.artist, t1.song
)

select * from cte_billboard where "row_number_artist" = 1;

-- criar um rank com lag e lead e first_value e last_value

with cte_billboard as (
	select distinct t1.artist, t1.song
from public."Billboard" as t1
order by t1.artist, t1.song
)
select * ,row_number() over(order by artist, song) as "row_number"
		 ,row_number() over(partition by artist order by artist, song) as "row_number_artist"
		 ,rank() over(partition by artist order by artist, song) as "rank"
		 ,lag(song, 1) over(partition by artist order by artist, song) as "lag_song"
		 ,lead(song, 1) over(partition by artist order by artist, song) as "lead_song"
		 ,first_value(song) over(partition by artist order by artist, song) as "first_song"
		 ,last_value(song) over(partition by artist order by artist, 
		 song range between unbounded preceding and unbounded following) as "last_song"
from cte_billboard


-- PEGAR A PRIMEIRA VEZ QUE OS ARTISTAS ENTRARAM NA BILLBOARD -- 
-- QUAL A PRIMEIRA POSIÇÃO DELES --

-- CRIAÇÃO DE TABELA PARA A VIEW --

create table tb_web_site as (

with cte_dedup_artist as (
select
	t1."date",
	       t1."rank",
	       t1.artist,
	       row_number() over(partition by artist
order by
	artist,
	"date") as dedup
from
	public."Billboard" as t1
order by
	t1.artist,
	t1."date"
)
select
	t1."date",
	t1."rank",
	t1.artist
from
	cte_dedup_artist as t1
where
	t1.dedup = 1

)


create table tb_artist as(
	select t1."date",
	       t1."rank",
	       t1.artist,
	       t1.song
	from public."Billboard" as t1
	where t1.artist = 'AC/DC'
	order by t1.artist, t1.song, t1."date"
);

drop table tb_artist;

select * from tb_artist;


-- CRIAÇÃO DE VIEW --

create view vw_artist as(
	with cte_dedup_artist as(
		select t1."date",
			   t1."rank",
			   t1.artist,
			   row_number() over(partition by artist order by artist, "date") as dedup
	    from tb_artist as t1
	    order by t1.artist, t1."date"
	)
	
	select t1."date",
		   t1."rank",
		   t1.artist
    from cte_dedup_artist as t1
    where t1.dedup = 1

);

select * from vw_artist;


-- INSERIR VALOR NA TABELA DA VIEW --

insert into tb_artist(
	select t1."date",
		   t1."rank",
		   t1.artist,
		   t1.song
    from public."Billboard" as t1
    where t1.artist like 'Elvis%'
    order by t1.artist, t1.song, t1."date"
);

select * from vw_artist;


-- vw_song --

create view vw_song as(
	with cte_dedup_artist as(
		select t1."date",
			   t1."rank",
			   t1.artist,
			   t1.song,
			   row_number() over(partition by artist, song order by artist, song, "date") as dedup
		from tb_artist as t1
		order by t1.artist, t1.song, t1."date"
)
	
	select t1."date",
		   t1."rank",
		   t1.artist,
		   t1.song
    from cte_dedup_artist as t1
    where t1.dedup = 1
);


select * from vw_song;



-- adele --

insert into tb_artist(
	select t1."date",
		   t1."rank",
		   t1.artist, 
		   t1.song
 	from public."Billboard" as t1
 	where t1.artist like 'Adele%'
 	order by t1.artist, t1.song, t1."date"
);

select * from vw_artist;
select * from vw_song;














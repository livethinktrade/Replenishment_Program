/*VERSION 1*/
create table item_support (
	
	upc varchar(12) primary key,
	upc_11_digit varchar(11) unique not NULL,
	season char(2) not null,
	type varchar(30),
	style varchar(15),
	gm varchar(5),
	additional varchar(30),
	code_qb varchar(20),
	item_desc varchar(50),
	unit char(5),
	salesrank numeric,
	total_case_size numeric,
	shipped_per_case numeric,
	mupc varchar(12) not NULL,
	item_group_desc varchar(50),
	display_size varchar(30),
	size varchar(3),
	availability integer not NULL
	
	
);

-- Table: public.delivery

-- DROP TABLE IF EXISTS public.delivery;

CREATE TABLE IF NOT EXISTS public.delivery
(
    id serial NOT NULL PRIMARY KEY,
	transition_date_range character varying(10) COLLATE pg_catalog."default" NOT NULL,
    type character varying(11) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
    upc character varying(12) COLLATE pg_catalog."default" NOT NULL,
    store integer NOT NULL,
    qty numeric NOT NULL,
	store_type character varying(20) NOT NULL,
    num numeric
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.delivery
    OWNER to postgres;
-- Index: fki_C

-- DROP INDEX IF EXISTS public."fki_C";


CREATE INDEX IF NOT EXISTS "fki_C"
    ON public.delivery USING btree
    (upc COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: public.sales

-- DROP TABLE IF EXISTS public.sales;

CREATE TABLE IF NOT EXISTS public.sales
(
    id serial NOT NULL,
    transition_date_range character varying(10) COLLATE pg_catalog."default" NOT NULL,
    store_year integer,
	/*for non kroger stores, store_week is set to date*/
    store_week date NOT NULL,
    store_number integer NOT NULL,
    upc character varying(12) COLLATE pg_catalog."default" NOT NULL,
    sales numeric NOT NULL,
    qty integer NOT NULL,
    current_year integer,
    current_week integer,
    store_type character varying(20) COLLATE pg_catalog."default" NOT NULL,

    CONSTRAINT sales_pk PRIMARY KEY (id),
    CONSTRAINT sales_fk FOREIGN KEY (upc)
        REFERENCES public.item_support (upc_11_digit) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)


TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sales
    OWNER to postgres;



create table case_capacity (
	
store_type varchar(20),
store numeric primary key,
rack_4w numeric,
rack_1w numeric,
rack_2w numeric,
rack_pw numeric,
carded numeric,
long_hanging_top numeric,
long_hanging_dress numeric,	
case_capacity numeric,
notes varchar(50),
initial varchar(10)

);

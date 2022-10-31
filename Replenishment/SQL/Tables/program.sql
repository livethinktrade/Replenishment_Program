/*VERSION 2*/
create table master_planogram (
	
program_id varchar(15) PRIMARY KEY,
cd_ay numeric not null,
cd_sn numeric not null,
lht_ay numeric not null,
lht_sn numeric not null,
lhd_ay numeric not null,
lhd_sn numeric not null,
lhp_ay numeric not null,
lhp_sn numeric not null,
total_cases numeric not null

);

create table store_info (
	
store_id integer,
initial varchar(10) NOT NULL,
notes varchar(50) NOT NULL,
store_type character varying(20) NOT NULL,

primary key(store_id, store_type)

);

create table store_program (
	
store_program_id serial PRIMARY KEY,
store_id integer NOT NULL,
program_id varchar(15) NOT NULL,
activity varchar(10),
store_type character varying(20) NOT NULL,

FOREIGN KEY (program_id) REFERENCES public.master_planogram (program_id),
FOREIGN KEY (store_id, store_type) REFERENCES store_info(store_id, store_type)

);

create table item_support2 (

	season char(2) not null,
	category varchar(15) not null,
    type varchar(15) not null,
    style varchar(15),
    additional varchar(15),
    display_size varchar(25),
    pog_type varchar(60),
    upc varchar(12),
    code varchar(20) primary key,
    code_qb varchar(20) not null,
    unique_replen_code varchar(12),
    case_size numeric not null,
    item_group_desc varchar(50),
    item_desc varchar(50),
    packing numeric,
    upc_11_digit varchar(11)
	
	
);

CREATE TABLE delivery
(
    id serial NOT NULL PRIMARY KEY,
    transition_year integer,
	transition_season character varying(3) NOT NULL,
    type character varying(11) NOT NULL,
    date date NOT NULL,
    upc character varying(12) NOT NULL,
    store integer NOT NULL,
    qty numeric NOT NULL,
	store_type character varying(20) NOT NULL,
    num numeric,
    code varchar(20) NOT NULL,

    FOREIGN KEY (code) REFERENCES public.item_support2 (code),
    FOREIGN KEY (store, store_type) REFERENCES store_info(store_id, store_type)

);

CREATE TABLE sales
(
    id serial NOT NULL PRIMARY KEY,
    transition_year integer,
    transition_season character varying(3) NOT NULL,
    store_year integer,
    date date not null,
    store_week integer NOT NULL,
    store_number integer NOT NULL,
    upc character varying(12) NOT NULL,
    sales numeric NOT NULL,
    qty integer NOT NULL,
    current_year integer,
    current_week integer,
    code varchar(20) NOT NULL,
    store_type character varying(20) NOT NULL

);

CREATE TABLE item_approval
(
    code character varying(20),
    store_price numeric NOT NULL,
    recommend_replen character varying(3) not null,
    store_type character varying(20),

    primary key (store_type, code)

);

CREATE TABLE inventory
(
    code character varying(20) not null PRIMARY KEY,
    on_hand numeric not null
);

CREATE TABLE item_size
(
    code character varying(20) not null PRIMARY KEY,
    size varchar(15)
)

/*sql statement to find mask sales for midwest stores */

Create view as midwest_mask (
 SELECT sum(sales2.sales) AS sum
   FROM fresh_encounter.sales2
  WHERE (sales2.store_number < 400 OR sales2.store_number > 499) AND sales2.store_year = 2022);

Create Table store_program_history
(
    store_program_id integer NOT NULL,
    store_id integer NOT NULL,
    program_id varchar(15) NOT NULL,
    activity varchar(10),
    store_type character varying(20) NOT NULL,
    date_updated date,
    history_id serial primary key,
	notes character varying(100)


)


Create Table year_week_verify (
    store_year integer not null,
    store_week integer not null,
    store_type character varying(20),
    PRIMARY KEY(store_year, store_week, store_type)
)

Create Table bandaids (
    type character varying(11) NOT NULL,
    store_id integer not null,
    item_group_desc varchar(50),
    qty numeric not null,
    date_created date NOT NULL,
    effective_date date NOT NULL,
    store_type character varying(20) NOT NULL,
    reason character varying(100) NOT NULL,

    PRIMARY KEY(store_id, item_group_desc, effective_date)
)

Create Table kroger_periods (

    kroger_period integer not null,
    kroger_week integer primary key

)

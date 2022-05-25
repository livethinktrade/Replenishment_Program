/*VERSION 2*/
create table program (
	
program_id varchar(5) PRIMARY KEY,
carded numeric NOT NULL,
long_hanging_top numeric NOT NULL,
long_hanging_dress numeric NOT NULL

);


create table store (
	
store_id integer PRIMARY KEY,
initial varchar(5) NOT NULL,
notes varchar(50) NOT NULL

);


create table store_program (
	
store_program_id serial PRIMARY KEY,
store_id integer NOT NULL,
program_id varchar(5) NOT NULL,

FOREIGN KEY (program_id) REFERENCES program (program_id),
FOREIGN KEY (store_id) REFERENCES store(store_id)

);


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

CREATE TABLE delivery
(
    id serial NOT NULL PRIMARY KEY,
	transition_date_range character varying(10) NOT NULL,
    type character varying(11) NOT NULL,
    date date NOT NULL,
    upc character varying(12) NOT NULL,
    store integer NOT NULL,
    qty numeric NOT NULL,
	store_type character varying(20) NOT NULL,
    num integer,

    FOREIGN KEY (upc) REFERENCES item_support (upc),
    FOREIGN KEY (store) REFERENCES store(store_id)

);



CREATE TABLE sales
(
    id serial NOT NULL PRIMARY KEY,
    transition_date_range character varying(10) NOT NULL,
    store_year integer,
	/*for non kroger stores, store_week is set to date*/
    store_week date NOT NULL,
    store_number integer NOT NULL,
    upc character varying(12) NOT NULL,
    sales numeric NOT NULL,
    qty integer NOT NULL,
    current_year integer,
    current_week integer,
    store_type character varying(20) NOT NULL,

    FOREIGN KEY (upc) REFERENCES item_support (upc_11_digit),
    FOREIGN KEY (store_number) REFERENCES store(store_id)
);


CREATE TABLE item_approval
(
    upc character varying(12) NOT NULL PRIMARY KEY,
    price_level numeric NOT NULL,

    FOREIGN KEY (upc) REFERENCES item_support (upc)

)




/*sameple data
insert into program (program_id,carded,long_hanging_top,long_hanging_dress)
values ('4W',2,1,1);

insert into program (program_id,carded,long_hanging_top,long_hanging_dress)
values ('2W',2,0,0);

insert into store (store_id, initial, notes) 
values (33,'ALB','DO NOT SHIP');

insert into store_program (store_id, program_id)
values (33, '4W');

insert into store_program (store_id, program_id)
values (33, '2W');

*/
-- Table: public.sales

-- DROP TABLE IF EXISTS public.sales;

CREATE TABLE IF NOT EXISTS public.sales
(
    id integer NOT NULL DEFAULT nextval('sales_id_seq'::regclass),
    transition_date_range character varying(10) COLLATE pg_catalog."default" NOT NULL,
    store_year integer,
    store_week integer NOT NULL,
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
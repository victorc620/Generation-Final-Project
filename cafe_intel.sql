-- Adminer 4.8.1 PostgreSQL 13.1 (Debian 13.1-1.pgdg100+1) dump

DROP TABLE IF EXISTS "cafe";
DROP SEQUENCE IF EXISTS cafe_id_cafe_seq;
CREATE SEQUENCE cafe_id_cafe_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 3 CACHE 1;

CREATE TABLE "public"."cafe" (
    "id_cafe" integer DEFAULT nextval('cafe_id_cafe_seq') NOT NULL,
    "name_cafe" character varying(45),
    CONSTRAINT "cafe_pkey" PRIMARY KEY ("id_cafe")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders";
DROP SEQUENCE IF EXISTS orders_id_order_seq;
CREATE SEQUENCE orders_id_order_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."orders" (
    "id_order" integer DEFAULT nextval('orders_id_order_seq') NOT NULL,
    "id_cafe" integer,
    "date" timestamp,
    "payment_type" character varying(45),
    "total_price" real,
    CONSTRAINT "orders_pkey" PRIMARY KEY ("id_order")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders_products";
CREATE TABLE "public"."orders_products" (
    "id_order" integer,
    "id_product" integer,
    "quantity_purchased" integer
) WITH (oids = false);


DROP TABLE IF EXISTS "products";
DROP SEQUENCE IF EXISTS products_products_id_seq;
CREATE SEQUENCE products_products_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."products" (
    "id_product" integer DEFAULT nextval('products_products_id_seq') NOT NULL,
    "name" character varying(45),
    "size" character varying(45),
    "price" double precision,
    CONSTRAINT "products_pkey" PRIMARY KEY ("id_product")
) WITH (oids = false);


ALTER TABLE ONLY "public"."orders" ADD CONSTRAINT "id_cafe" FOREIGN KEY (id_cafe) REFERENCES cafe(id_cafe) NOT DEFERRABLE;

ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "id_order" FOREIGN KEY (id_order) REFERENCES orders(id_order) NOT DEFERRABLE;
ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "id_product" FOREIGN KEY (id_product) REFERENCES products(id_product) NOT DEFERRABLE;

-- 2022-01-28 14:09:22.877378+00

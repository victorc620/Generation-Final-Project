-- Adminer 4.8.1 PostgreSQL 13.1 (Debian 13.1-1.pgdg100+1) dump

DROP TABLE IF EXISTS "cafe";
DROP SEQUENCE IF EXISTS cafe_id_cafe_seq;
CREATE SEQUENCE cafe_id_cafe_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."cafe" (
    "id_cafe" integer DEFAULT nextval('cafe_id_cafe_seq') NOT NULL,
    "name_cafe" character varying(45) NOT NULL,
    CONSTRAINT "cafe_pkey" PRIMARY KEY ("id_cafe")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders";
DROP SEQUENCE IF EXISTS orders_id_order_seq;
CREATE SEQUENCE orders_id_order_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."orders" (
    "id_order" integer DEFAULT nextval('orders_id_order_seq') NOT NULL,
    "id_cafe" integer NOT NULL,
    "date" timestamp NOT NULL,
    "payment_type" character varying(45) NOT NULL,
    "total_price" real NOT NULL,
    CONSTRAINT "orders_pkey" PRIMARY KEY ("id_order")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders_products";
CREATE TABLE "public"."orders_products" (
    "id_order" integer NOT NULL,
    "id_product" integer NOT NULL,
    "quantity_purchased" integer NOT NULL
) WITH (oids = false);


DROP TABLE IF EXISTS "price_cafe";
DROP SEQUENCE IF EXISTS price_cafe_id_price_seq;
CREATE SEQUENCE price_cafe_id_price_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."price_cafe" (
    "id_price" integer DEFAULT nextval('price_cafe_id_price_seq') NOT NULL,
    "id_cafe" integer,
    "id_product" integer,
    "price" real NOT NULL,
    "data" timestamp NOT NULL,
    "active" integer DEFAULT '1' NOT NULL,
    CONSTRAINT "price_cafe_pkey" PRIMARY KEY ("id_price")
) WITH (oids = false);


DROP TABLE IF EXISTS "products";
DROP SEQUENCE IF EXISTS products_products_id_seq;
CREATE SEQUENCE products_products_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."products" (
    "id_product" integer DEFAULT nextval('products_products_id_seq') NOT NULL,
    "name" character varying(45) NOT NULL,
    "size" character varying(45),
    "flavor" character varying(45),
    "price" double precision NOT NULL,
    CONSTRAINT "products_pkey" PRIMARY KEY ("id_product")
) WITH (oids = false);


ALTER TABLE ONLY "public"."orders" ADD CONSTRAINT "id_cafe" FOREIGN KEY (id_cafe) REFERENCES cafe(id_cafe) NOT DEFERRABLE;

ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "id_order" FOREIGN KEY (id_order) REFERENCES orders(id_order) NOT DEFERRABLE;
ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "id_product" FOREIGN KEY (id_product) REFERENCES products(id_product) NOT DEFERRABLE;

ALTER TABLE ONLY "public"."price_cafe" ADD CONSTRAINT "id_cafe" FOREIGN KEY (id_cafe) REFERENCES cafe(id_cafe) NOT DEFERRABLE;
ALTER TABLE ONLY "public"."price_cafe" ADD CONSTRAINT "id_product" FOREIGN KEY (id_product) REFERENCES products(id_product) NOT DEFERRABLE;

-- 2022-01-27 12:56:35.547404+00

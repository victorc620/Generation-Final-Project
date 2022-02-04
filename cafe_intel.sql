-- Adminer 4.8.1 PostgreSQL 13.1 (Debian 13.1-1.pgdg100+1) dump

DROP TABLE IF EXISTS "cafe";
DROP SEQUENCE IF EXISTS cafe_id_cafe_seq;
CREATE SEQUENCE cafe_id_cafe_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 3 CACHE 1;

CREATE TABLE "public"."cafe" (
    "cafe_id" integer DEFAULT nextval('cafe_id_cafe_seq') NOT NULL,
    "location" character varying(45),
    CONSTRAINT "cafe_pkey" PRIMARY KEY ("cafe_id")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders";
CREATE TABLE "public"."orders" (
    "order_id" character varying(45) NOT NULL,
    "cafe_id" integer,
    "date" timestamp,
    "payment_type" character varying(45),
    "total_price" real,
    CONSTRAINT "orders_pkey" PRIMARY KEY ("order_id")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders_products";
CREATE TABLE "public"."orders_products" (
    "order_id" character varying(45) NOT NULL,
    "product_id" integer,
    "quantity_purchased" integer
) WITH (oids = false);


DROP TABLE IF EXISTS "products";
DROP SEQUENCE IF EXISTS products_products_id_seq;
CREATE SEQUENCE products_products_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."products" (
    "product_id" integer DEFAULT nextval('products_products_id_seq') NOT NULL,
    "name" character varying(45),
    "price" double precision,
    CONSTRAINT "products_pkey" PRIMARY KEY ("product_id")
) WITH (oids = false);


ALTER TABLE ONLY "public"."orders" ADD CONSTRAINT "id_cafe" FOREIGN KEY (cafe_id) REFERENCES cafe(cafe_id) NOT DEFERRABLE;

ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "id_product" FOREIGN KEY (product_id) REFERENCES products(product_id) NOT DEFERRABLE;
ALTER TABLE ONLY "public"."orders_products" ADD CONSTRAINT "orders_products_id_order_fkey" FOREIGN KEY (order_id) REFERENCES orders(order_id) NOT DEFERRABLE;

-- 2022-02-04 11:33:41.534847+00

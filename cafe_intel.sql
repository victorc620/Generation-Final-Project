DROP TABLE IF EXISTS "cafe";
CREATE TABLE "public"."cafe" (
    "cafe_id" character varying(45) NOT NULL,
    "location" character varying(45) NOT NULL,
    CONSTRAINT "cafe_cafe_id" PRIMARY KEY ("cafe_id")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders";
CREATE TABLE "public"."orders" (
    "order_id" character varying(45) NOT NULL,
    "cafe_id" character varying(45) NOT NULL,
    "date" timestamp NOT NULL,
    "payment_type" character varying(45) NOT NULL,
    "total_price" double precision NOT NULL,
    CONSTRAINT "orders_pkey" PRIMARY KEY ("order_id")
) WITH (oids = false);


DROP TABLE IF EXISTS "orders_products";
CREATE TABLE "public"."orders_products" (
    "order_id" character varying(45) NOT NULL,
    "product_id" character varying(45) NOT NULL,
    "quantity_purchased" integer
) WITH (oids = false);


DROP TABLE IF EXISTS "products";
CREATE TABLE "public"."products" (
    "product_id" character varying(45) NOT NULL,
    "products" character varying(45) NOT NULL,
    "products_price" double precision NOT NULL,
    CONSTRAINT "products_pkey" PRIMARY KEY ("product_id")
) WITH (oids = false);
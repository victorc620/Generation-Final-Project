-- Adminer 4.8.1 MySQL 8.0.27 dump

SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `cafe`;
CREATE TABLE `cafe` (
  `id_cafe` int NOT NULL,
  `name_cafe` varchar(45) NOT NULL,
  PRIMARY KEY (`id_cafe`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `id_customer` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  PRIMARY KEY (`id_customer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
  `id_order` int NOT NULL AUTO_INCREMENT,
  `id_cafe` int NOT NULL,
  `id_customer` int NOT NULL,
  `date` date NOT NULL,
  `payment_type` varchar(45) NOT NULL,
  PRIMARY KEY (`id_order`),
  KEY `id_cafe _idx` (`id_cafe`),
  KEY `id_customer _idx` (`id_customer`),
  CONSTRAINT `id_cafe ` FOREIGN KEY (`id_cafe`) REFERENCES `cafe` (`id_cafe`),
  CONSTRAINT `id_customer ` FOREIGN KEY (`id_customer`) REFERENCES `customer` (`id_customer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `orders_products`;
CREATE TABLE `orders_products` (
  `id_order` int NOT NULL,
  `id_product` int NOT NULL,
  KEY `id_order_idx` (`id_order`),
  KEY `id_product_idx` (`id_product`),
  CONSTRAINT `id_order` FOREIGN KEY (`id_order`) REFERENCES `orders` (`id_order`),
  CONSTRAINT `id_product` FOREIGN KEY (`id_product`) REFERENCES `products` (`products_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `products`;
CREATE TABLE `products` (
  `products_id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  `size` varchar(45) DEFAULT NULL,
  `flavor` varchar(45) DEFAULT NULL,
  `price` float NOT NULL,
  PRIMARY KEY (`products_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- 2022-01-25 16:43:19

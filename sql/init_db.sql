
CREATE DATABASE IF NOT EXISTS `crypto_etl_db`;

USE `crypto_etl_db`;

DROP TABLE IF EXISTS `crypto_data`;

CREATE TABLE `crypto_data` (
    `coin_name` VARCHAR(50) NOT NULL,
    `date_key` INT NOT NULL,
    `currency` VARCHAR(10),
    `price` DECIMAL(18, 2) NOT NULL,
    `volume` DECIMAL(30, 2) NOT NULL,

    PRIMARY KEY (`coin_name`, `date_key`, `currency`)
);
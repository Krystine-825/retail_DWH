-- Tạo database cho Data Warehouse
CREATE DATABASE retail_dwh;
\c retail_dwh;

-- Tạo các schema theo kiến trúc Medallion
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
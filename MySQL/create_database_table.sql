CREATE DATABASE nombreBasedeDatos;

use nombreBasedeDatos;

CREATE TABLE Tabla_Prueba
(
Nombre varchar(10),
Apellido varchar(20) primary key not null,
Posicion int
);

CREATE TABLE Users
(
id int primary key,
username varchar(20),
emai varchar(50)
);

CREATE TABLE ListaCompra
(
Producto char(20) primary key,
Cantidad int,
Precio float,
Prioridad char(20),
Destinatario char(20)
);

CREATE TABLE sincant
(
id int,
username char(50),
emai char(50)
);

CREATE TABLE Videogames
(
Game varchar(50) primary key not null,
Developer varchar(50),
Price float,
Console varchar(50),
Year_out int,
Score float
);

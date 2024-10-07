. .\TinySqlClient.ps1
Send-SQLCommand -command "CREATE DATABASE Universidad"
Send-SQLCommand -command "SET DATABASE Universidad"
Send-SQLCommand -command "CREATE TABLE Estudiante (ID INTEGER, Nombre VARCHAR(30), PrimerApellido VARCHAR(30), SegundoApellido VARCHAR(30), FechaNacimiento DATETIME)"
Send-SQLCommand -command "INSERT INTO Estudiante VALUES (1, 'Isaac', 'Ramirez', 'Herrera', '2000-01-01 01:02:00')"
Send-SQLCommand -command "INSERT INTO Estudiante VALUES (2, 'Juan', 'Ramirez', 'X', '2000-01-01 01:02:00')"
Send-SQLCommand -command "INSERT INTO Estudiante VALUES (3, 'Pedro', 'Herrera', 'Y', '2000-01-01 01:02:00')"
Send-SQLCommand -command "CREATE INDEX Estudiante_Id ON Estudiante(ID) OF TYPE BTREE"
Send-SQLCommand -command "INSERT INTO Estudiante VALUES (1, 'Andrés', 'Ramirez', 'Herrera', '2000-01-01 01:02:00')"
Send-SQLCommand -command "SELECT * FROM Estudiante WHERE ID = 2"
Send-SQLCommand -command "SELECT Nombre FROM Estudiante WHERE ID = 2"
Send-SQLCommand -command "SELECT * FROM Estudiante WHERE PrimerApellido LIKE '%mire%' ORDER BY Nombre DESC"
Send-SQLCommand -command "DELETE FROM Estudiante WHERE ID = 1" 
Send-SQLCommand -command "UPDATE Estudiante SET Nombre = 'Felipe' WHERE ID = 2" 
Send-SQLCommand -command "SELECT * FROM Estudiante" 

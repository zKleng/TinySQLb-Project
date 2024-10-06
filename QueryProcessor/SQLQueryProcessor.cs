using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;
using System.Collections.Generic;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            if (sentence.StartsWith("CREATE TABLE"))
            {
                // Extraer el nombre de la tabla y las columnas
                var tableName = ExtractTableNameFromCreate(sentence);
                var columns = ExtractColumns(sentence);

                // Define un nombre de base de datos por defecto, o extráelo de algún otro lugar
                string databaseName = "TESTDB";

                // Crear la operación de CreateTable con los parámetros extraídos
                var createTableOperation = new CreateTable(databaseName, tableName, columns);

                // Ejecutar la operación
                return createTableOperation.Execute().Status;
            }
            else if (sentence.StartsWith("INSERT INTO"))
            {
                // Extraer el nombre de la tabla y los valores
                var tableName = ExtractTableNameFromInsert(sentence);
                var values = ExtractValues(sentence);

                // Llamar a la clase Insert para realizar la operación
                var insertOperation = new Insert("TESTDB", tableName, values);

                // Ejecutar la operación
                return insertOperation.Execute().Status;
            }
            else if (sentence.StartsWith("SELECT"))
            {
                // Extraer el nombre de la tabla de la sentencia SELECT
                var tableName = ExtractTableNameFromSelect(sentence);
                string databaseName = "TESTDB"; // Nombre de base de datos por defecto

                // Llamar a la clase Select para realizar la operación
                var selectOperation = new Select();
                return selectOperation.Execute(databaseName, tableName);
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }

        // Función para extraer el nombre de la tabla de una sentencia CREATE TABLE
        private static string ExtractTableNameFromCreate(string sentence)
        {
            int startIndex = sentence.IndexOf("CREATE TABLE") + "CREATE TABLE".Length;
            int endIndex = sentence.IndexOf("(");
            return sentence.Substring(startIndex, endIndex - startIndex).Trim();
        }

        // Función para extraer el nombre de la tabla de una sentencia INSERT INTO
        private static string ExtractTableNameFromInsert(string sentence)
        {
            int startIndex = sentence.IndexOf("INSERT INTO") + "INSERT INTO".Length;
            int endIndex = sentence.IndexOf("VALUES");
            return sentence.Substring(startIndex, endIndex - startIndex).Trim();
        }

        // Función para extraer el nombre de la tabla de una sentencia SELECT
        private static string ExtractTableNameFromSelect(string sentence)
        {
            int startIndex = sentence.IndexOf("FROM") + "FROM".Length;
            return sentence.Substring(startIndex).Trim();
        }

        // Función para extraer la definición de las columnas en una sentencia CREATE TABLE
        // Función para extraer la definición de las columnas en una sentencia CREATE TABLE
        private static List<Column> ExtractColumns(string sentence)
        {
            int startIndex = sentence.IndexOf("(") + 1;
            int endIndex = sentence.LastIndexOf(")");

            // Verificar si los índices son válidos
            if (startIndex == -1 || endIndex == -1 || startIndex >= endIndex)
            {
                Console.WriteLine("Error al extraer columnas: no se encontró una definición válida.");
                return new List<Column>();
            }

            // Extrae la parte de la sentencia que contiene las columnas
            string columnsPart = sentence.Substring(startIndex, endIndex - startIndex).Trim();
            Console.WriteLine($"Extrayendo columnas de: {columnsPart}");

            // Divide las columnas por comas
            var columnsList = columnsPart.Split(',');

            List<Column> columns = new List<Column>();

            foreach (var columnDefinition in columnsList)
            {
                var parts = columnDefinition.Trim().Split(' ');

                if (parts.Length >= 2)
                {
                    string columnName = parts[0];
                    string columnType = parts[1];
                    int columnSize = 0;

                    // Si el tipo es VARCHAR, intenta extraer el tamaño
                    if (columnType.StartsWith("VARCHAR"))
                    {
                        // Buscar el paréntesis de apertura
                        int sizeStartIndex = columnType.IndexOf("(");
                        int sizeEndIndex = columnType.IndexOf(")");

                        if (sizeStartIndex != -1 && sizeEndIndex != -1)
                        {
                            var sizePart = columnType.Substring(sizeStartIndex + 1, sizeEndIndex - sizeStartIndex - 1);
                            if (int.TryParse(sizePart, out int size))
                            {
                                columnSize = size;
                            }
                            // Normalizar el tipo de columna sin el tamaño
                            columnType = "VARCHAR";
                        }
                    }
                    else if (columnType == "INTEGER")
                    {
                        columnSize = sizeof(int);  // Establecer un tamaño fijo para INTEGER.
                    }

                    // Crea el objeto columna y lo añade a la lista
                    columns.Add(new Column(columnName, columnType, columnSize));
                    Console.WriteLine($"Columna extraída: {columnName}, Tipo: {columnType}, Tamaño: {columnSize}");
                }
                else
                {
                    Console.WriteLine("Error: definición de columna inválida.");
                }
            }

            return columns;
        }


        // Función para extraer los valores de una sentencia INSERT INTO
        private static List<object> ExtractValues(string sentence)
        {
            // Encontrar la parte de la sentencia que contiene los valores
            int startIndex = sentence.IndexOf("VALUES") + "VALUES".Length;

            // Extraer la parte que contiene los valores y eliminar paréntesis iniciales y finales
            string valuesPart = sentence.Substring(startIndex).Trim();

            // Quitar los paréntesis exteriores si existen
            if (valuesPart.StartsWith("(") && valuesPart.EndsWith(")"))
            {
                valuesPart = valuesPart.Substring(1, valuesPart.Length - 2);
            }

            // Separar los valores por comas
            var valuesList = valuesPart.Split(',');

            List<object> values = new List<object>();

            foreach (var value in valuesList)
            {
                string trimmedValue = value.Trim().Trim('\''); // Eliminar espacios y comillas simples

                // Intentar convertir el valor a entero, si es posible
                if (int.TryParse(trimmedValue, out int intValue))
                {
                    values.Add(intValue);  // Añadir entero si es un número
                }
                else
                {
                    values.Add(trimmedValue);  // Añadir cadena si es texto
                }
            }

            return values;
        }




    }
}



using System.Collections.Generic;
using StoreDataManager;
using Entities;
using System;

namespace QueryProcessor.Operations
{
    internal class Insert
    {
        private string _databaseName;
        private string _tableName;
        private List<object> _values;

        public Insert(string databaseName, string tableName, List<object> values)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _values = values;
        }

        // Método que ejecuta la operación de inserción
        internal OperationResult Execute()
        {
            try
            {
                // Verificar si la base de datos existe
                var dbResult = Store.GetInstance().SetDatabase(_databaseName);
                if (dbResult.Status == OperationStatus.Error)
                {
                    return dbResult; // Retornar el resultado si la base de datos no existe
                }

                // Verificar si la tabla existe
                var columns = Store.GetInstance().GetTableDefinition(_databaseName, _tableName);
                if (columns.Count == 0)
                {
                    Console.WriteLine($"Error: The table '{_tableName}' does not exist in the database '{_databaseName}'.");
                    return new OperationResult(OperationStatus.Error, $"Table '{_tableName}' does not exist.");
                }

                // Validar el número y tipo de columnas
                if (_values.Count != columns.Count)
                {
                    Console.WriteLine("Column count mismatch between provided values and table definition.");
                    return new OperationResult(OperationStatus.Warning, "Column count mismatch.");
                }

                for (int i = 0; i < columns.Count; i++)
                {
                    var column = columns[i];
                    var value = _values[i];

                    if (column.Type == "INTEGER" && !(value is int))
                    {
                        Console.WriteLine($"Error: The value for column '{column.Name}' should be an integer.");
                        return new OperationResult(OperationStatus.Error, $"Invalid data type for column '{column.Name}'. Expected INTEGER.");
                    }

                    if (column.Type == "VARCHAR" && !(value is string))
                    {
                        Console.WriteLine($"Error: The value for column '{column.Name}' should be a string.");
                        return new OperationResult(OperationStatus.Error, $"Invalid data type for column '{column.Name}'. Expected VARCHAR.");
                    }

                    if (column.Type == "DATETIME" && !(value is string dateTimeValue && DateTime.TryParse(dateTimeValue, out _)))
                    {
                        Console.WriteLine($"Error: The value for column '{column.Name}' should be a valid DateTime string.");
                        return new OperationResult(OperationStatus.Error, $"Invalid data type for column '{column.Name}'. Expected DATETIME string.");
                    }
                }

                // Llamar al método Insert en Store para insertar los datos
                return Store.GetInstance().Insert(_databaseName, _tableName, _values);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during insertion: {ex.Message}");
                return new OperationResult(OperationStatus.Error, "An error occurred during the insert operation.");
            }
        }
    }
}


using Entities;
using StoreDataManager;
using System.Collections.Generic;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        private string _databaseName;
        private string _tableName;
        private List<Column> _columns;

        // Constructor que recibe los parámetros necesarios
        public CreateTable(string databaseName, string tableName, List<Column> columns)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _columns = columns;
        }

        // Ejecuta el método CreateTable en Store con los parámetros
        internal OperationResult Execute()
        {
            Console.WriteLine($"Ejecutando CreateTable para la tabla {_tableName} en la base de datos {_databaseName}.");
            foreach (var column in _columns)
            {
                Console.WriteLine($"Columna a crear: {column.Name}, Tipo: {column.Type}, Tamaño: {column.Size}");
            }
            return Store.GetInstance().CreateTable(_databaseName, _tableName, _columns);
        }
    }

}


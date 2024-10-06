using System.Collections.Generic;
using StoreDataManager;
using Entities;

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
            // Llamar al método Insert en Store para insertar los datos
            return Store.GetInstance().Insert(_databaseName, _tableName, _values);
        }
    }
}


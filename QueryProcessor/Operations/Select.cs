using Entities;
using StoreDataManager;
using System;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public OperationStatus Execute(string databaseName, string tableName)
        {
            // Llamar al método Select en Store con los parámetros correctos
            return Store.GetInstance().Select(databaseName, tableName).Status;
        }
    }
}



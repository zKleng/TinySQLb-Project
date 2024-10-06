using System;
using System.IO;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class DropTable
    {
        private readonly string _databaseName;
        private readonly string _tableName;

        public DropTable(string databaseName, string tableName)
        {
            _databaseName = databaseName;
            _tableName = tableName;
        }

        public OperationResult Execute()
        {
            var dataPath = Store.GetInstance().GetDataPath();
            var tablePath = $@"{dataPath}\{_databaseName}\{_tableName}.table";
            Console.WriteLine($"Attempting to drop table: {_tableName} in database: {_databaseName}");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("Error: Table does not exist.");
                return new OperationResult(OperationStatus.Error, "Table does not exist.");
            }

            // Check if the table is empty
            using (FileStream stream = File.Open(tablePath, FileMode.Open))
            {
                if (stream.Length > 0)
                {
                    Console.WriteLine("Error: Table is not empty. Cannot drop a non-empty table.");
                    return new OperationResult(OperationStatus.Error, "Table is not empty. Cannot drop a non-empty table.");
                }
            }

            // Delete the table file
            try
            {
                File.Delete(tablePath);
                Console.WriteLine($"Table {_tableName} successfully dropped from database {_databaseName}.");
                return new OperationResult(OperationStatus.Success, "Table dropped successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting table {_tableName}: {ex.Message}");
                return new OperationResult(OperationStatus.Error, "Error deleting table: " + ex.Message);
            }
        }
    }
}

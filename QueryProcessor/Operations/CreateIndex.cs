using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class CreateIndex
    {
        private readonly string _databaseName;
        private readonly string _tableName;
        private readonly string _columnName;
        private readonly string _indexName;
        private readonly string _indexType; // BTREE o BST

        public CreateIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _columnName = columnName;
            _indexName = indexName;
            _indexType = indexType;
        }

        public OperationResult Execute()
        {
            var store = Store.GetInstance();

            // Obtener la definición de las columnas de la tabla
            var columns = store.GetTableDefinition(_databaseName, _tableName);
            var column = columns.Find(c => c.Name == _columnName);

            if (column == null)
            {
                Console.WriteLine("Error: La columna especificada no existe en la tabla.");
                return new OperationResult(OperationStatus.Error, "La columna especificada no existe en la tabla.");
            }

            // Verificar si la columna ya tiene un índice
            if (store.IndexExists(_databaseName, _tableName, _columnName))
            {
                Console.WriteLine("Error: Ya existe un índice en esta columna.");
                return new OperationResult(OperationStatus.Error, "Ya existe un índice en esta columna.");
            }

            // Verificar si la columna tiene valores repetidos antes de crear el índice
            if (store.HasDuplicateValues(_databaseName, _tableName, _columnName))
            {
                Console.WriteLine("Error: No se puede crear un índice sobre una columna con valores repetidos.");
                return new OperationResult(OperationStatus.Error, "No se puede crear un índice sobre una columna con valores repetidos.");
            }

            // Crear el índice en memoria y guardar la referencia en el catálogo del sistema
            var indexPath = $@"{store.GetDataPath()}\{_databaseName}\{_indexName}.index";
            using (FileStream stream = File.Open(indexPath, FileMode.Create))
            {
                Console.WriteLine($"Creando índice {_indexName} de tipo {_indexType} en la columna {_columnName} de la tabla {_tableName}.");
                // Aquí deberías implementar la lógica para crear el árbol B o el BST, y almacenar la referencia.
                // Por simplicidad, esto solo crea un archivo vacío.
            }

            // Actualizar el catálogo del sistema con la información del índice creado
            store.UpdateSystemCatalogWithIndex(_databaseName, _tableName, _columnName, _indexName, _indexType);

            return new OperationResult(OperationStatus.Success, "Índice creado con éxito.");
        }
    }
}


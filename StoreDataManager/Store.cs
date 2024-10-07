using Entities;
using System.Data.Common;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        public static Store GetInstance()
        {
            lock (_lock)
            {
                if (instance == null)
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";

        public string GetDataPath()
        {
            return DataPath;
        }

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog()
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }

        public OperationResult CreateDatabase(string databaseName)
        {
            var databasePath = $@"{DataPath}\{databaseName}";

            if (Directory.Exists(databasePath))
            {
                Console.WriteLine($"Error: La base de datos {databaseName} ya existe.");
                return new OperationResult(OperationStatus.Error, "Database already exists.");
            }

            try
            {
                // Crear el directorio para la base de datos
                Directory.CreateDirectory(databasePath);
                Console.WriteLine($"Base de datos {databaseName} creada exitosamente en {databasePath}.");
                return new OperationResult(OperationStatus.Success, "Database created successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la base de datos {databaseName}: {ex.Message}");
                return new OperationResult(OperationStatus.Error, $"Failed to create database: {ex.Message}");
            }
        }

        public OperationResult SetDatabase(string databaseName)
        {
            var databasePath = $@"{DataPath}\{databaseName}";

            if (Directory.Exists(databasePath))
            {
                Console.WriteLine($"Base de datos {databaseName} establecida exitosamente.");
                return new OperationResult(OperationStatus.Success, "Database exists.");
            }
            else
            {
                Console.WriteLine($"Error: La base de datos {databaseName} no existe.");
                return new OperationResult(OperationStatus.Error, "Database does not exist.");
            }
        }

        public OperationResult CreateTable(string databaseName, string tableName, List<Column> columns)
        {
            var databasePath = $@"{DataPath}\{databaseName}";
            if (!Directory.Exists(databasePath))
            {
                return new OperationResult(OperationStatus.Error, "Database does not exist.");
            }

            Console.WriteLine($"Creando tabla {tableName} en la base de datos {databaseName} con {columns.Count} columnas.");

            // Verifica si hay columnas definidas antes de continuar
            if (columns.Count == 0)
            {
                return new OperationResult(OperationStatus.Error, "No se han definido columnas para la tabla.");
            }

            // Crea la tabla: Solo crea un archivo vacío como indicador de la existencia de la tabla
            var tablePath = $@"{databasePath}\{tableName}.table";
            if (File.Exists(tablePath))
            {
                return new OperationResult(OperationStatus.Error, "Table already exists.");
            }

            using (FileStream stream = File.Open(tablePath, FileMode.Create))
            {
                Console.WriteLine($"Archivo de la tabla {tableName} creado.");
            }

            // Actualiza el catálogo del sistema con la nueva tabla y sus columnas
            UpdateSystemCatalog(databaseName, tableName, columns);

            // Aquí agregas el código para verificar las columnas
            var createdColumns = GetTableDefinition(databaseName, tableName);
            Console.WriteLine($"Número de columnas en la tabla creada: {createdColumns.Count}");
            foreach (var column in createdColumns)
            {
                Console.WriteLine($"Columna: {column.Name}, Tipo: {column.Type}, Tamaño: {column.Size}");
            }

            return new OperationResult(OperationStatus.Success, "Table created successfully.");
        }

        private void UpdateSystemCatalog(string databaseName, string tableName, List<Column> columns)
        {
            var catalogPath = $@"{SystemCatalogPath}\SystemTables.table";
            using (FileStream stream = File.Open(catalogPath, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                writer.Write(databaseName);
                writer.Write(tableName);
                foreach (var column in columns)
                {
                    Console.WriteLine($"Escribiendo columna: {column.Name}, Tipo: {column.Type}, Tamaño: {column.Size}");
                    writer.Write(column.Name);
                    writer.Write(column.Type);
                    writer.Write(column.Size);
                }
            }
        }

        public List<Column> GetTableDefinition(string databaseName, string tableName)
        {
            var catalogPath = $@"{SystemCatalogPath}\SystemTables.table";
            using (FileStream stream = File.Open(catalogPath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                List<Column> columns = new List<Column>();
                while (stream.Position < stream.Length)
                {
                    try
                    {
                        string dbName = reader.ReadString();
                        string tblName = reader.ReadString();

                        if (dbName == databaseName && tblName == tableName)
                        {
                            Console.WriteLine("Leyendo columnas para la tabla " + tableName);
                            while (stream.Position < stream.Length)
                            {
                                string columnName = reader.ReadString();
                                string columnType = reader.ReadString();
                                int columnSize = reader.ReadInt32();

                                columns.Add(new Column(columnName, columnType, columnSize));
                                Console.WriteLine($"Columna leída: {columnName}, Tipo: {columnType}, Tamaño: {columnSize}");
                            }
                            break;  // Detener el bucle una vez que se han leído las columnas de la tabla
                        }

                    }
                    catch (EndOfStreamException)
                    {
                        Console.WriteLine("Error: Fin del archivo mientras se leían las columnas.");
                        break;
                    }
                    catch (IOException e)
                    {
                        Console.WriteLine($"Error de lectura de archivo: {e.Message}");
                        break;
                    }
                }

                if (columns.Count == 0)
                {
                    Console.WriteLine("No se encontraron columnas para la tabla especificada.");
                }
                return columns;
            }
        }

        public OperationResult Insert(string databaseName, string tableName, List<object> values)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.table";
            Console.WriteLine($"Inserting into table: {tableName} in database: {databaseName}");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("Table does not exist.");
                return new OperationResult(OperationStatus.Error, "Table does not exist.");
            }

            // Valida los valores contra la definición de la tabla
            var columns = GetTableDefinition(databaseName, tableName);
            Console.WriteLine($"Number of columns in the table: {columns.Count}");
            foreach (var column in columns)
            {
                Console.WriteLine($"Column: {column.Name}, Type: {column.Type}, Size: {column.Size}");
            }

            if (columns.Count != values.Count)
            {
                Console.WriteLine("Column count mismatch.");
                return new OperationResult(OperationStatus.Warning, "Column count mismatch.");
            }

            // Verificar si el ID ya existe (solo para columnas con nombre 'ID')
            int idIndex = columns.FindIndex(c => c.Name.Equals("ID", StringComparison.OrdinalIgnoreCase));
            if (idIndex >= 0)
            {
                int newIdValue = (int)values[idIndex];
                if (DoesValueExist(databaseName, tableName, "ID", newIdValue.ToString()))
                {
                    Console.WriteLine($"Error: ID '{newIdValue}' ya existe en la tabla '{tableName}'.");
                    return new OperationResult(OperationStatus.Error, $"Duplicate ID value '{newIdValue}' is not allowed.");
                }
            }

            long position;
            using (FileStream stream = File.Open(tablePath, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                position = stream.Position; // Guardar la posición actual para los índices

                for (int i = 0; i < columns.Count; i++)
                {
                    var column = columns[i];
                    var value = values[i];
                    Console.WriteLine($"Processing column: {column.Name}, Type: {column.Type}, Value: {value}");

                    if (column.Type == "INTEGER" && value is int)
                    {
                        writer.Write((int)value);
                        Console.WriteLine($"Wrote integer value: {value}");
                    }
                    else if (column.Type == "VARCHAR" && value is string)
                    {
                        string stringValue = ((string)value);

                        // Truncar la cadena si excede el tamaño de la columna
                        if (stringValue.Length > column.Size)
                        {
                            stringValue = stringValue.Substring(0, column.Size);
                            Console.WriteLine($"String value truncated to: {stringValue}");
                        }

                        // Ajustar la cadena al tamaño de la columna con espacios
                        stringValue = stringValue.PadRight(column.Size);
                        writer.Write(stringValue.ToCharArray());
                        Console.WriteLine($"Wrote string value: {stringValue}");
                    }
                    else if (column.Type == "DATETIME" && value is string)
                    {
                        // Intentar parsear el valor de cadena a DateTime
                        if (DateTime.TryParse((string)value, out DateTime dateTimeValue))
                        {
                            writer.Write(dateTimeValue.ToBinary());
                            Console.WriteLine($"Wrote DateTime value: {dateTimeValue}");
                        }
                        else
                        {
                            Console.WriteLine($"Invalid DateTime value for column {column.Name}");
                            return new OperationResult(OperationStatus.Error, $"Invalid DateTime value for column {column.Name}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Invalid value for column {column.Name}");
                        return new OperationResult(OperationStatus.Error, $"Invalid value for column {column.Name}");
                    }
                }
            }

            // Actualizar los índices después de insertar la fila
            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var value = values[i];

                if (IndexExists(databaseName, tableName, column.Name))
                {
                    // Actualizar el índice con la nueva posición de la fila
                    UpdateIndex(databaseName, tableName, column.Name, value, position);
                }
            }

            return new OperationResult(OperationStatus.Success, "Row inserted successfully.");
        }

        // Método para verificar si un valor ya existe en una columna específica
        private bool DoesValueExist(string databaseName, string tableName, string columnName, string value)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.table";
            using (FileStream stream = File.Open(tablePath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                var columns = GetTableDefinition(databaseName, tableName);
                int columnIndex = columns.FindIndex(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));

                if (columnIndex == -1)
                {
                    Console.WriteLine($"Error: Column '{columnName}' not found in table '{tableName}'.");
                    return false;
                }

                // Leer todas las filas y verificar si el valor ya existe
                while (stream.Position < stream.Length)
                {
                    for (int i = 0; i < columns.Count; i++)
                    {
                        var column = columns[i];
                        if (column.Type == "INTEGER")
                        {
                            int intValue = reader.ReadInt32();
                            if (i == columnIndex && intValue.ToString() == value)
                            {
                                return true; // El valor ya existe
                            }
                        }
                        else if (column.Type == "VARCHAR")
                        {
                            char[] charArray = reader.ReadChars(column.Size);
                            string columnValue = new string(charArray).Trim();
                            if (i == columnIndex && columnValue == value)
                            {
                                return true; // El valor ya existe
                            }
                        }
                        else if (column.Type == "DATETIME")
                        {
                            long binaryValue = reader.ReadInt64();
                            DateTime dateTimeValue = DateTime.FromBinary(binaryValue);
                            if (i == columnIndex && dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss") == value)
                            {
                                return true; // El valor ya existe
                            }
                        }
                    }
                }
            }

            return false;
        }


        // Método para actualizar el índice
        private void UpdateIndex(string databaseName, string tableName, string columnName, object value, long position)
        {
            var indexPath = $@"{SystemCatalogPath}\{databaseName}_{tableName}_{columnName}.index";

            using (FileStream stream = File.Open(indexPath, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                // Validar si el valor es null
                string valueToWrite = value?.ToString() ?? string.Empty;

                writer.Write(valueToWrite);
                writer.Write(position);
                Console.WriteLine($"Índice actualizado para la columna {columnName} con valor {valueToWrite} en posición {position}.");
            }
        }

        public OperationResult Select(string databaseName, string tableName)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.table";

            if (!File.Exists(tablePath))
            {
                return new OperationResult(OperationStatus.Error, "Table does not exist.");
            }

            List<string> rows = new List<string>();

            using (FileStream stream = File.Open(tablePath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                var columns = GetTableDefinition(databaseName, tableName);

                // Salta la parte de las columnas
                stream.Seek(columns.Sum(c => c.Name.Length + c.Type.Length + sizeof(int)), SeekOrigin.Begin);

                // Leer todas las filas del archivo
                while (stream.Position < stream.Length)
                {
                    List<string> row = new List<string>();

                    foreach (var column in columns)
                    {
                        if (stream.Position >= stream.Length)
                        {
                            Console.WriteLine("Fin inesperado del archivo mientras se leían las columnas.");
                            return new OperationResult(OperationStatus.Error, "Unexpected end of file while reading the table.");
                        }

                        if (column.Type == "INTEGER")
                        {
                            if (stream.Length - stream.Position < sizeof(int))
                            {
                                Console.WriteLine($"Error al leer INTEGER para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, "Unexpected end of file while reading an INTEGER.");
                            }

                            int intValue = reader.ReadInt32();
                            row.Add(intValue.ToString());
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {intValue}");
                        }
                        else if (column.Type == "VARCHAR")
                        {
                            if (stream.Length - stream.Position < column.Size)
                            {
                                Console.WriteLine($"Error al leer VARCHAR para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, $"Unexpected end of file while reading VARCHAR column {column.Name}.");
                            }

                            char[] charArray = reader.ReadChars(column.Size);
                            string columnValue = new string(charArray).Trim();
                            row.Add(columnValue);
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {columnValue}");
                        }
                        else if (column.Type == "DATETIME")
                        {
                            if (stream.Length - stream.Position < sizeof(long))
                            {
                                Console.WriteLine($"Error al leer DATETIME para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, "Unexpected end of file while reading a DATETIME.");
                            }

                            long binaryValue = reader.ReadInt64();
                            DateTime dateTimeValue = DateTime.FromBinary(binaryValue);
                            row.Add(dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss"));
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {dateTimeValue}");
                        }
                    }
                    rows.Add(string.Join(", ", row));
                }
            }

            return new OperationResult(OperationStatus.Success, string.Join("\n", rows));
        }

        public bool IndexExists(string databaseName, string tableName, string columnName)
        {
            // Verifica en el catálogo del sistema si ya existe un índice en la columna especificada
            var indexPath = $@"{SystemCatalogPath}\{databaseName}_{tableName}_{columnName}.index";
            return File.Exists(indexPath);
        }

        public bool HasDuplicateValues(string databaseName, string tableName, string columnName)
        {
            // Verificar si hay valores duplicados en la columna especificada de la tabla
            // Aquí puedes leer la tabla y buscar duplicados
            return false; // Suponiendo que no hay duplicados por defecto
        }

        public void UpdateSystemCatalogWithIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            var catalogPath = $@"{SystemCatalogPath}\SystemIndexes.table";
            using (FileStream stream = File.Open(catalogPath, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                writer.Write(databaseName);
                writer.Write(tableName);
                writer.Write(columnName);
                writer.Write(indexName);
                writer.Write(indexType);
                Console.WriteLine($"Índice {indexName} creado en la columna {columnName} de la tabla {tableName}.");
            }
        }
    }
}
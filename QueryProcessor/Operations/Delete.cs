using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class Delete
    {
        private string _databaseName;
        private string _tableName;
        private string _whereCondition;

        public Delete(string databaseName, string tableName, string whereCondition)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _whereCondition = whereCondition;
        }

        internal OperationResult Execute()
        {
            var tablePath = $@"{Store.GetInstance().GetDataPath()}\{_databaseName}\{_tableName}.table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("Table does not exist.");
                return new OperationResult(OperationStatus.Error, "Table does not exist.");
            }

            bool rowsDeleted = false;

            // Utilizar MemoryStream para manejar el archivo temporalmente
            using (FileStream stream = File.Open(tablePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            using (BinaryReader reader = new BinaryReader(stream))
            using (MemoryStream tempStream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(tempStream))
            {
                var columns = Store.GetInstance().GetTableDefinition(_databaseName, _tableName);

                // Leer todas las filas del archivo
                while (stream.Position < stream.Length)
                {
                    Dictionary<string, string> row = new Dictionary<string, string>();

                    // Leer cada columna de la fila
                    foreach (var column in columns)
                    {
                        if (column.Type == "INTEGER")
                        {
                            int intValue = reader.ReadInt32();
                            row[column.Name] = intValue.ToString();
                        }
                        else if (column.Type == "VARCHAR")
                        {
                            char[] charArray = reader.ReadChars(column.Size);
                            string columnValue = new string(charArray).Trim();
                            row[column.Name] = columnValue;
                        }
                        else if (column.Type == "DATETIME")
                        {
                            long binaryValue = reader.ReadInt64();
                            DateTime dateTimeValue = DateTime.FromBinary(binaryValue);
                            row[column.Name] = dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss");
                        }

                    }

                    // Evaluar la condición WHERE para determinar si eliminar la fila
                    if (string.IsNullOrEmpty(_whereCondition) || !EvaluateWhereCondition(row, _whereCondition))
                    {
                        // Escribir la fila en el stream temporal si no debe ser eliminada
                        foreach (var column in columns)
                        {
                            if (column.Type == "INTEGER")
                            {
                                writer.Write(int.Parse(row[column.Name]));
                            }
                            else if (column.Type == "VARCHAR")
                            {
                                string value = row[column.Name].PadRight(column.Size);
                                writer.Write(value.ToCharArray());
                            }
                            else if (column.Type == "DATETIME")
                            {
                                DateTime dateValue = DateTime.Parse(row[column.Name]);
                                writer.Write(dateValue.ToBinary());
                            }

                        }
                    }
                    else
                    {
                        // Indicar que se ha eliminado una fila
                        rowsDeleted = true;
                    }
                }

                // Si alguna fila fue eliminada, actualizar el archivo original
                if (rowsDeleted)
                {
                    stream.SetLength(0); // Limpiar el archivo original
                    tempStream.Seek(0, SeekOrigin.Begin);
                    tempStream.CopyTo(stream); // Copiar los datos actualizados al archivo original
                }
            }

            return new OperationResult(OperationStatus.Success, rowsDeleted ? "Rows deleted successfully." : "No rows matched the condition.");
        }

        // Método para evaluar la condición WHERE para una fila específica
        // Método para evaluar la condición WHERE para una fila específica
        // Método para evaluar la condición WHERE para una fila específica
        // Método para evaluar la condición WHERE para una fila específica
        private bool EvaluateWhereCondition(Dictionary<string, string> row, string whereCondition)
        {
            if (string.IsNullOrEmpty(whereCondition)) return true;

            // Utilizar una expresión regular para dividir la condición en columna, operador y valor
            var regex = new System.Text.RegularExpressions.Regex(@"^(\w+)\s*(=|!=|>|<|LIKE)\s*(.+)$");
            var match = regex.Match(whereCondition);

            if (!match.Success || match.Groups.Count != 4)
            {
                Console.WriteLine("Error: Condición WHERE no válida.");
                return false;
            }

            string column = match.Groups[1].Value.Trim();
            string operatorValue = match.Groups[2].Value.Trim();
            string value = match.Groups[3].Value.Trim();

            // Eliminar las comillas simples al inicio y al final si están presentes
            if (value.StartsWith("'") && value.EndsWith("'"))
            {
                value = value.Substring(1, value.Length - 2);
            }

            if (row.ContainsKey(column))
            {
                switch (operatorValue)
                {
                    case "=":
                        return row[column] == value;
                    case "!=":
                        return row[column] != value;
                    case ">":
                        return int.TryParse(row[column], out int rowValue) && int.TryParse(value, out int conditionValue) && rowValue > conditionValue;
                    case "<":
                        return int.TryParse(row[column], out int rowVal) && int.TryParse(value, out int condVal) && rowVal < condVal;
                    case "LIKE":
                        string pattern = value.Replace("%", ".*"); // Convertir '%' en un patrón regex
                        return System.Text.RegularExpressions.Regex.IsMatch(row[column], pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                    default:
                        return false;
                }
            }
            return false;
        }

    }
}


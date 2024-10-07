using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class Update
    {
        private string _databaseName;
        private string _tableName;
        private Dictionary<string, string> _columnUpdates; // Almacena columna y nuevo valor
        private string _whereCondition;

        public Update(string databaseName, string tableName, Dictionary<string, string> columnUpdates, string whereCondition)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _columnUpdates = columnUpdates;
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

            List<string> updatedRows = new List<string>();
            bool rowsUpdated = false;

            // Utilizar MemoryStream para manejar el archivo temporalmente
            using (FileStream stream = File.Open(tablePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            using (BinaryReader reader = new BinaryReader(stream))
            using (MemoryStream tempStream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(tempStream))
            {
                var columns = Store.GetInstance().GetTableDefinition(_databaseName, _tableName);

                while (stream.Position < stream.Length)
                {
                    Dictionary<string, string> row = new Dictionary<string, string>();

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

                    if (EvaluateWhereCondition(row, _whereCondition))
                    {
                        foreach (var columnUpdate in _columnUpdates)
                        {
                            if (row.ContainsKey(columnUpdate.Key))
                            {
                                row[columnUpdate.Key] = columnUpdate.Value;
                                rowsUpdated = true;
                            }
                        }
                    }

                    // Reescribir la fila actualizada en el nuevo stream
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

                // Guardar las filas actualizadas
                if (rowsUpdated)
                {
                    stream.SetLength(0); // Limpiar el archivo original
                    tempStream.Seek(0, SeekOrigin.Begin);
                    tempStream.CopyTo(stream); // Copiar los datos actualizados al archivo original
                }
            }

            return new OperationResult(OperationStatus.Success, rowsUpdated ? "Rows updated successfully." : "No rows matched the condition.");
        }

        private bool EvaluateWhereCondition(Dictionary<string, string> row, string whereCondition)
        {
            if (string.IsNullOrEmpty(whereCondition)) return true;

            string[] operators = { "=", "!=", ">", "<", "LIKE" };
            foreach (var op in operators)
            {
                int opIndex = whereCondition.IndexOf(op);
                if (opIndex != -1)
                {
                    string column = whereCondition.Substring(0, opIndex).Trim();
                    string value = whereCondition.Substring(opIndex + op.Length).Trim();

                    // Eliminar las comillas simples al inicio y al final si están presentes
                    if (value.StartsWith("'") && value.EndsWith("'"))
                    {
                        value = value.Substring(1, value.Length - 2);
                    }

                    if (row.ContainsKey(column))
                    {
                        switch (op)
                        {
                            case "=":
                                return row[column] == value;
                            case "!=":
                                return row[column] != value;
                            case ">":
                                if (int.TryParse(row[column], out int rowValue) && int.TryParse(value, out int conditionValue))
                                {
                                    return rowValue > conditionValue;
                                }
                                break;
                            case "<":
                                if (int.TryParse(row[column], out int rowVal) && int.TryParse(value, out int condVal))
                                {
                                    return rowVal < condVal;
                                }
                                break;
                            case "LIKE":
                                string pattern = value.Replace("%", ".*"); // Convertir '%' en un patrón regex para LIKE
                                return System.Text.RegularExpressions.Regex.IsMatch(row[column], pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                            default:
                                return false;
                        }
                    }
                }
            }
            return false;
        }



    }
}



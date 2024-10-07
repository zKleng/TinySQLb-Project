using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        private string _databaseName;
        private SelectDetails _selectDetails;

        public Select(string databaseName, SelectDetails selectDetails)
        {
            _databaseName = databaseName;
            _selectDetails = selectDetails;
        }

        // Método que ejecuta la operación de selección
        internal OperationResult Execute()
        {
            var tablePath = $@"{Store.GetInstance().GetDataPath()}\{_databaseName}\{_selectDetails.TableName}.table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("Table does not exist.");
                return new OperationResult(OperationStatus.Error, "Table does not exist.");
            }

            List<string> rows = new List<string>();
            using (FileStream stream = File.Open(tablePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                var columns = Store.GetInstance().GetTableDefinition(_databaseName, _selectDetails.TableName);

                // Leer todas las filas del archivo y filtrar según la condición WHERE
                while (stream.Position < stream.Length)
                {
                    Dictionary<string, string> row = new Dictionary<string, string>();
                    foreach (var column in columns)
                    {
                        if (stream.Position >= stream.Length)
                        {
                            Console.WriteLine("Error: Fin inesperado del archivo mientras se leían los datos.");
                            return new OperationResult(OperationStatus.Error, "Unexpected end of file while reading data.");
                        }

                        if (column.Type == "INTEGER")
                        {
                            if (stream.Length - stream.Position < sizeof(int))
                            {
                                Console.WriteLine($"Error al leer INTEGER para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, $"Unexpected end of file while reading INTEGER column {column.Name}.");
                            }

                            int intValue = reader.ReadInt32();
                            row[column.Name] = intValue.ToString();
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {intValue}");
                        }
                        else if (column.Type == "DATETIME")
                        {
                            if (stream.Length - stream.Position < sizeof(long))
                            {
                                Console.WriteLine($"Error al leer DATETIME para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, $"Unexpected end of file while reading DATETIME column {column.Name}.");
                            }

                            long binaryValue = reader.ReadInt64();
                            DateTime dateTimeValue = DateTime.FromBinary(binaryValue);
                            row[column.Name] = dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss");
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {dateTimeValue}");
                        }

                        else if (column.Type == "VARCHAR")
                        {
                            if (stream.Length - stream.Position < column.Size)
                            {
                                Console.WriteLine($"Error al leer VARCHAR para la columna: {column.Name}");
                                return new OperationResult(OperationStatus.Error, $"Unexpected end of file while reading VARCHAR column {column.Name}.");
                            }

                            char[] charArray = reader.ReadChars(column.Size);
                            string columnValue = new string(charArray).TrimEnd();
                            row[column.Name] = columnValue;
                            Console.WriteLine($"Leyendo columna: {column.Name}, Valor: {columnValue}");
                        }
                    }

                    // Verificar si la fila cumple con la condición WHERE
                    if (EvaluateWhereCondition(row, _selectDetails.WhereCondition))
                    {
                        var selectedValues = GetSelectedValues(row, _selectDetails.Columns);
                        rows.Add(string.Join(", ", selectedValues));
                    }
                }
            }

            // Aplicar ORDER BY si está especificado
            if (!string.IsNullOrEmpty(_selectDetails.OrderByColumn))
            {
                rows = ApplyOrderBy(rows, _selectDetails.OrderByColumn, _selectDetails.OrderByDirection);
            }

            return new OperationResult(OperationStatus.Success, string.Join("\n", rows));
        }


        // Método para obtener los valores seleccionados según las columnas especificadas
        private List<string> GetSelectedValues(Dictionary<string, string> row, List<string> columns)
        {
            List<string> selectedValues = new List<string>();

            if (columns.Count == 0) // Si columnas == "*", selecciona todos los valores
            {
                selectedValues.AddRange(row.Values);
            }
            else
            {
                foreach (var column in columns)
                {
                    if (row.ContainsKey(column))
                    {
                        selectedValues.Add(row[column]);
                    }
                }
            }

            return selectedValues;
        }

        // Método para evaluar la condición WHERE para una fila específica
        // Método para evaluar la condición WHERE para una fila específica
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
                    string value = whereCondition.Substring(opIndex + op.Length).Trim().Trim('\'', '\"');

                    if (row.ContainsKey(column))
                    {
                        switch (op)
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
                                return row[column].Contains(value.Replace("%", ""));
                            default:
                                return false;
                        }
                    }
                }
            }
            return false;
        }
        // Método para aplicar el ORDER BY a la lista de filas
        private List<string> ApplyOrderBy(List<string> rows, string orderByColumn, string direction)
        {
            var columns = Store.GetInstance().GetTableDefinition(_databaseName, _selectDetails.TableName);
            var columnIndex = columns.FindIndex(c => c.Name == orderByColumn);

            if (columnIndex == -1)
            {
                Console.WriteLine($"Error: La columna {orderByColumn} no se encontró para ORDER BY.");
                return rows;
            }

            rows.Sort((a, b) =>
            {
                var aValue = a.Split(',')[columnIndex].Trim();
                var bValue = b.Split(',')[columnIndex].Trim();

                var columnType = columns[columnIndex].Type;

                int comparison = 0;
                if (columnType == "INTEGER")
                {
                    comparison = int.Parse(aValue).CompareTo(int.Parse(bValue));
                }
                else if (columnType == "DATETIME")
                {
                    comparison = DateTime.Parse(aValue).CompareTo(DateTime.Parse(bValue));
                }
                else
                {
                    comparison = string.Compare(aValue, bValue);
                }

                return direction == "DESC" ? -comparison : comparison;
            });

            return rows;
        }

    }
}






using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

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
                        }
                    }

                    // Verificar si la fila cumple con la condición WHERE
                    bool matchesCondition = EvaluateWhereCondition(row, _selectDetails.WhereCondition);
                    if (matchesCondition)
                    {
                        var selectedValues = GetSelectedValues(row, _selectDetails.Columns);
                        if (selectedValues.Count > 0)
                        {
                            Console.WriteLine(string.Join(", ", selectedValues));
                            rows.Add(string.Join(", ", selectedValues));
                        }
                    }
                }
            }

            // Aplicar ORDER BY si está especificado
            if (!string.IsNullOrEmpty(_selectDetails.OrderByColumn))
            {
                rows = ApplyOrderBy(rows, _selectDetails.OrderByColumn, _selectDetails.OrderByDirection);
            }

            return new OperationResult(OperationStatus.Success, rows.Count > 0 ? string.Join("\n", rows) : "No matching records found.");
        }


        // Método para obtener los valores seleccionados según las columnas especificadas
        private List<string> GetSelectedValues(Dictionary<string, string> row, List<string> columns)
        {
            List<string> selectedValues = new List<string>();

            if (columns.Count == 0 || (columns.Count == 1 && columns[0] == "*")) // Si columnas == "*", selecciona todos los valores
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
        private bool EvaluateWhereCondition(Dictionary<string, string> row, string whereCondition)
        {
            if (string.IsNullOrEmpty(whereCondition)) return true;

            // Utilizar una expresión regular para dividir la condición en columna, operador y valor
            var regex = new Regex(@"^(\w+)\s*(=|!=|>|<|LIKE)\s*(.+)$");
            var match = regex.Match(whereCondition);

            if (!match.Success || match.Groups.Count != 4)
            {
                Console.WriteLine("Error: Condición WHERE no válida.");
                return false;
            }

            string column = match.Groups[1].Value.Trim();
            string operatorValue = match.Groups[2].Value.Trim();
            string value = match.Groups[3].Value.Trim(new char[] { '\'', '"' });

            if (row.ContainsKey(column))
            {
                Console.WriteLine($"Evaluando condición WHERE: Columna={column}, Operador={operatorValue}, Valor={value}, ValorFila={row[column]}");

                // Verificar si el valor en la fila y el valor de la condición son ambos numéricos
                bool isRowValueNumeric = int.TryParse(row[column], out int rowValue);
                bool isConditionValueNumeric = int.TryParse(value, out int conditionValue);

                if (isRowValueNumeric && isConditionValueNumeric)
                {
                    // Comparación numérica para columnas INTEGER
                    switch (operatorValue)
                    {
                        case "=":
                            return rowValue == conditionValue;
                        case "!=":
                            return rowValue != conditionValue;
                        case ">":
                            return rowValue > conditionValue;
                        case "<":
                            return rowValue < conditionValue;
                        default:
                            return false;
                    }
                }
                else if (DateTime.TryParse(row[column], out DateTime rowDateValue) && DateTime.TryParse(value, out DateTime conditionDateValue))
                {
                    // Comparación para columnas DATETIME
                    switch (operatorValue)
                    {
                        case "=":
                            return rowDateValue == conditionDateValue;
                        case "!=":
                            return rowDateValue != conditionDateValue;
                        case ">":
                            return rowDateValue > conditionDateValue;
                        case "<":
                            return rowDateValue < conditionDateValue;
                        default:
                            return false;
                    }
                }
                else
                {
                    // Comparación para columnas VARCHAR
                    switch (operatorValue)
                    {
                        case "=":
                            return string.Equals(row[column], value, StringComparison.OrdinalIgnoreCase);
                        case "!=":
                            return !string.Equals(row[column], value, StringComparison.OrdinalIgnoreCase);
                        case "LIKE":
                            string pattern = "^" + Regex.Escape(value).Replace("%", ".*") + "$";
                            return Regex.IsMatch(row[column], pattern, RegexOptions.IgnoreCase);
                        default:
                            return false;
                    }
                }
            }
            else
            {
                Console.WriteLine($"Columna {column} no encontrada en la fila.");
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
                var aValues = a.Split(new[] { ',' }, StringSplitOptions.None);
                var bValues = b.Split(new[] { ',' }, StringSplitOptions.None);

                if (columnIndex >= aValues.Length || columnIndex >= bValues.Length)
                {
                    Console.WriteLine("Error: Índice de columna fuera de rango durante ORDER BY.");
                    return 0;
                }

                var aValue = aValues[columnIndex].Trim();
                var bValue = bValues[columnIndex].Trim();

                var columnType = columns[columnIndex].Type;

                int comparison = 0;
                if (columnType == "INTEGER")
                {
                    if (int.TryParse(aValue, out int aIntValue) && int.TryParse(bValue, out int bIntValue))
                    {
                        comparison = aIntValue.CompareTo(bIntValue);
                    }
                }
                else if (columnType == "DATETIME")
                {
                    if (DateTime.TryParse(aValue, out DateTime aDateValue) && DateTime.TryParse(bValue, out DateTime bDateValue))
                    {
                        comparison = aDateValue.CompareTo(bDateValue);
                    }
                }
                else
                {
                    comparison = string.Compare(aValue, bValue, StringComparison.OrdinalIgnoreCase);
                }

                return direction == "DESC" ? -comparison : comparison;
            });

            return rows;
        }

    }
}




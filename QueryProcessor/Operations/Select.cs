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
            using (FileStream stream = File.Open(tablePath, FileMode.Open))
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
        private bool EvaluateWhereCondition(Dictionary<string, string> row, string whereCondition)
        {
            if (string.IsNullOrEmpty(whereCondition)) return true;

            // Dividir la condición en columna, operador y valor
            var parts = whereCondition.Split(new char[] { ' ', '=', '<', '>', '!' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2) return false;

            string column = parts[0].Trim();
            string value = parts[1].Trim();
            string operatorValue = whereCondition.Substring(column.Length, whereCondition.Length - column.Length - value.Length).Trim();

            // Comparar según el operador
            if (row.ContainsKey(column))
            {
                switch (operatorValue)
                {
                    case "=":
                        return row[column] == value;
                    case "<":
                        return int.Parse(row[column]) < int.Parse(value);
                    case ">":
                        return int.Parse(row[column]) > int.Parse(value);
                    case "<>":
                    case "!=":
                        return row[column] != value;
                    case "LIKE":
                        return row[column].Contains(value.Replace("%", ""));
                }
            }

            return false;
        }

        // Método para aplicar el ORDER BY a la lista de filas
        private List<string> ApplyOrderBy(List<string> rows, string orderByColumn, string direction)
        {
            // Realizar ordenamiento de las filas con base en la columna especificada
            rows.Sort((a, b) =>
            {
                var columns = Store.GetInstance().GetTableDefinition(_databaseName, _selectDetails.TableName);
                var columnIndex = columns.FindIndex(c => c.Name == orderByColumn);

                var aValue = a.Split(',')[columnIndex].Trim();
                var bValue = b.Split(',')[columnIndex].Trim();

                int comparison = string.Compare(aValue, bValue);
                return direction == "DESC" ? -comparison : comparison;
            });

            return rows;
        }
    }
}






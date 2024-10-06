using System.Collections.Generic;

namespace Entities
{
    public class UpdateDetails
    {
        public string TableName { get; set; } = string.Empty;
        public Dictionary<string, string> ColumnUpdates { get; set; } = new Dictionary<string, string>();
        public string WhereCondition { get; set; } = string.Empty;
    }
}


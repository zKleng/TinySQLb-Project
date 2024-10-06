using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    public class SelectDetails
    {
        public List<string> Columns { get; set; } = new List<string>();
        public string TableName { get; set; } = string.Empty;
        public string WhereCondition { get; set; } = string.Empty;
        public string OrderByColumn { get; set; } = string.Empty;
        public string OrderByDirection { get; set; } = "ASC";
    }
}


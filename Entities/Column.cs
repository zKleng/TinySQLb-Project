using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    public class Column
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public int Size { get; set; }

        // Constructor que recibe los valores y los asigna
        public Column(string name, string type, int size)
        {
            Name = name;
            Type = type;
            Size = size;
        }
    }
}


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    public class OperationResult
    {
        public OperationStatus Status { get; set; }
        public string Message { get; set; }

        public OperationResult(OperationStatus status, string message)
        {
            Status = status;
            Message = message;
        }
    }
}


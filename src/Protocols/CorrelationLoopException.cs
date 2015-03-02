using System;

namespace kafka4net.Protocols
{
    class CorrelationLoopException : Exception
    {
        public bool IsRequestedClose;
        public CorrelationLoopException(string message) : base(message)
        {
            
        }
    }
}

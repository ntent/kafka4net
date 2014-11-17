using System;

namespace kafka4net.Protocols
{
    class CorrelationLoopException : Exception
    {
        public CorrelationLoopException(string message) : base(message)
        {
            
        }
    }
}

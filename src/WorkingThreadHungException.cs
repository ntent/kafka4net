using System;

namespace kafka4net
{
    class WorkingThreadHungException : BrokerException
    {
        public WorkingThreadHungException(string message) : base(message) {}
    }
}

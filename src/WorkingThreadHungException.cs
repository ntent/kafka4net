using System;

namespace kafka4net
{
    public class WorkingThreadHungException : BrokerException
    {
        public WorkingThreadHungException(string message) : base(message) {}
    }
}

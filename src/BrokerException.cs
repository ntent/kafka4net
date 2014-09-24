using System;

namespace kafka4net
{
    public class BrokerException : Exception
    {
        public BrokerException(string message) : base(message) {}
    }
}

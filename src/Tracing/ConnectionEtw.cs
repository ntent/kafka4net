using System;
using System.Diagnostics.Tracing;


namespace kafka4net.Tracing
{
    [EventSource(Name = "kafka4net")]
    public class ConnectionEtw : EventSource
    {
        public static ConnectionEtw Log = new ConnectionEtw();

        public void Connecting(string host, int port)
        {
            Log.WriteEvent(1, host, port);
        }

        public void Connected(string host, int port)
        {
            Log.WriteEvent(2, host, port);
        }

        public void Errored(string host, int port)
        {
            Log.WriteEvent(3, host, port);
        }

        public void Disconnected(string host, int port)
        {
            Log.WriteEvent(4, host, port);
        }

    }
}

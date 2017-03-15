using System;
using System.Reactive.Concurrency;
using kafka4net.ConsumerImpl;

namespace kafka4net
{
    public class ClusterConfiguration
    {
        /// <summary>
        /// Subscription is performed asynchronously.
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        /// <param name="socketKeepAliveMs">value to use for TCP Socket Keepalive. Pass null or 0 to not set any keepalive.</param>
        public ClusterConfiguration(
            string seedBrokers,
            uint? socketKeepAliveMs = null)
        {
            SeedBrokers = seedBrokers;
            SocketKeepAliveMs = socketKeepAliveMs;
        }

        public uint? SocketKeepAliveMs { get; private set; }
        public string SeedBrokers { get; private set; }
    }
}

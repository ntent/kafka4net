using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using kafka4net;
using kafka4net.Utils;

namespace tests
{
    internal static class VagrantBrokerUtil
    {
        static readonly NLog.Logger _log = NLog.LogManager.GetCurrentClassLogger();

        public static Dictionary<string,string> BrokerIpToName = new Dictionary<string, string>
        {
            {"192.168.56.10","broker1"},
            {"192.168.56.20","broker2"},
            {"192.168.56.30","broker3"},
        }; 

        public static string GetBrokerNameFromIp(string ip)
        {
            if (!BrokerIpToName.ContainsKey(ip))
                throw new Exception("Unknown ip: " + ip);
            return BrokerIpToName[ip];
        }


        public static void CreateTopic(string topicName, int numPartitions, int replicationFactor)
        {
            _log.Info("Creating topic: '{0}'", topicName); 
            const string createTopicScript = "ssh -c '/opt/kafka_2.10-0.8.1.1/bin/kafka-topics.sh --create --topic {0} --partitions {1} --replication-factor {2} --zookeeper 192.168.56.2' broker1";
            Vagrant(string.Format(createTopicScript, topicName, numPartitions, replicationFactor));
            _log.Info("Created topic: '{0}'", topicName);
        }

        public static void RestartBrokers()
        {
            BrokerIpToName.Where(np=>!IsBrokerResponding(np.Key)).ForEach(np=>StartBroker(np.Value));
        }

        public static void StopBroker(string broker)
        {
            _log.Info("Stopping broker {0}", broker);
            Vagrant("ssh -c 'sudo service kafka stop' " + broker);
            _log.Info("Stopped broker {0}", broker);
        }

        public static void StopBrokerLeaderForPartition(Cluster cluster, string topic, int partition)
        {
            var brokerMeta = cluster.FindBrokerMetaForPartitionId(topic, partition);
            var brokerName = GetBrokerNameFromIp(brokerMeta.Host);
            StopBroker(brokerName);
            
        }

        public static void StartBroker(string broker)
        {
            _log.Info("Starting broker: '{0}'", broker);
            Vagrant("ssh -c 'sudo service kafka start' " + broker);
            _log.Info("Started broker: '{0}'", broker);
        }

        private static bool IsBrokerResponding(string ip)
        {
            try
            {
                var client = new TcpClient();
                return client.ConnectAsync(ip, 9092).Wait(TimeSpan.FromSeconds(2));
            }
            catch
            {
                return false;
            }
        }

        private static void Vagrant(string script)
        {
            // TODO: implement vagrant-control 
            var dir = AppDomain.CurrentDomain.BaseDirectory;
            dir = Path.Combine(dir, @"..\..\..\vagrant");
            dir = Path.GetFullPath(dir);
            var pi = new ProcessStartInfo(@"vagrant.exe", script)
            {
                CreateNoWindow = true,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                WorkingDirectory = dir
            };

            var p = Process.Start(pi);
            p.OutputDataReceived += (sender, args) => Console.WriteLine(args.Data);
            p.ErrorDataReceived += (sender, args) => Console.WriteLine(args.Data);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            p.WaitForExit();
        }
    }
}

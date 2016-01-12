using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
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

        static readonly string _kafkaVersion = File.ReadLines(
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\..\\vagrant\\scripts\\kafka_version.txt")
        ).First();

        public static string GetBrokerNameFromIp(string ip)
        {
            if (!BrokerIpToName.ContainsKey(ip))
                throw new Exception("Unknown ip: " + ip);
            return BrokerIpToName[ip];
        }


        public static void CreateTopic(string topicName, int numPartitions, int replicationFactor)
        {
            _log.Info("Creating topic: '{0}'", topicName); 
            string createTopicScript = "ssh -c '/opt/kafka_2.10-"+_kafkaVersion+"/bin/kafka-topics.sh --create --topic {0} --partitions {1} --replication-factor {2} --zookeeper 192.168.56.2' broker1";
            Vagrant(string.Format(createTopicScript, topicName, numPartitions, replicationFactor));
            _log.Info("Created topic: '{0}'", topicName);
        }

        public static void RebalanceLeadership()
        {
            _log.Info("Rebalancing Leadership");
            string rebalanceScript = "ssh -c '/opt/kafka_2.10-"+_kafkaVersion+"/bin/kafka-preferred-replica-election.sh --zookeeper 192.168.56.2' broker1";
            Vagrant(rebalanceScript);
            _log.Info("Rebalanced Leadership");
        }

        public static void DescribeTopic(string topic)
        {
            _log.Info("Getting topic description");
            string script = "ssh -c '/opt/kafka_2.10-" + _kafkaVersion + "/bin/kafka-topics.sh --zookeeper 192.168.56.2 --describe --topic " + topic + "' broker1";
            Vagrant(script);
            _log.Info("Got topic description");
        }

        public static void ReassignPartitions(Cluster cluster, string topic, int partition)
        {
            var brokerMeta = cluster.FindBrokerMetaForPartitionId(topic, partition);
            var brokerToMoveTo = brokerMeta.NodeId == 1 ? 2 : 1;

            var partitionsJson = string.Format("{{\"partitions\":[{{\"topic\":\"{0}\",\"partition\":{1},\"replicas\":[{2}]}}], \"version\":1}}",
                topic, partition, brokerToMoveTo);

            _log.Info(string.Format("Reassigning Partitions (topic {0}, partition {1}, from node {2} to node {3})", topic, partition, brokerMeta.NodeId, brokerToMoveTo));

            var generateJson = "ssh -c \"printf '" + partitionsJson.Replace("\"", @"\\\""") + "' >partitions-to-move.json\" broker1";
            Vagrant(generateJson);

            var reassignScript = "ssh -c '/opt/kafka_2.10-" + _kafkaVersion + "/bin/kafka-reassign-partitions.sh --zookeeper 192.168.56.2 --reassignment-json-file partitions-to-move.json --execute' broker1";
            Vagrant(reassignScript);

            _log.Info("Reassigned Partitions");
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

        public static string StopBrokerLeaderForPartition(Cluster cluster, string topic, int partition)
        {
            var brokerMeta = cluster.FindBrokerMetaForPartitionId(topic, partition);
            var brokerName = GetBrokerNameFromIp(brokerMeta.Host);
            StopBroker(brokerName);
            return brokerName;
        }

        public static void StartBroker(string broker)
        {
            _log.Info("Starting broker: '{0}'", broker);
            Vagrant("ssh -c 'sudo service kafka start' " + broker);

            // await for tcp to become accessible, because process start does not mean that server has done initializing and start listening
            while(!IsBrokerResponding(BrokerIpToName.Single(b => b.Value == broker).Key))
                Thread.Sleep(200);

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
            p.OutputDataReceived += (sender, args) => _log.Info(args.Data);
            p.ErrorDataReceived += (sender, args) => _log.Info(args.Data);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            p.WaitForExit();

            if(p.ExitCode != 0)
                throw new Exception("Vagrant failed with exit code " + p.ExitCode);
        }
    }
}

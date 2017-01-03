using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using log4net.Config;

namespace examples
{
    class Program
    {
        #region staticLog4netAppender
        const string StaticConsoleAppender = @"
            <log4net debug=""false"">
                <appender name=""ColoredConsoleAppender"" type=""log4net.Appender.ColoredConsoleAppender"">
                    <mapping>
                        <level value=""ERROR"" />
                        <foreColor value=""White"" />
                        <backColor value=""Red, HighIntensity"" />
                    </mapping>
                    <mapping>
                        <level value=""INFO"" />
                        <foreColor value=""White"" />
                    </mapping>
                    <mapping>
                        <level value=""DEBUG"" />
                        <foreColor value=""Blue"" />
                    </mapping>
                    <layout type=""log4net.Layout.PatternLayout"">
                        <conversionPattern value=""%date [%thread] %-5level %logger - %message%newline"" />
                    </layout>
                </appender>
                <logger name=""kafka4net"">
                    <level value=""Info"" />
                </logger>
                <root>
                    <level value=""Debug"" />
                    <appender-ref ref=""ColoredConsoleAppender"" />
                </root>
            </log4net>
                ";
        #endregion staticLog4netAppender

        private static ILog _log;

        static void Main(string[] args)
        {
            var log4NetConfig = ConfigurationManager.AppSettings.Get("LoggingConfigFile");
            if (string.IsNullOrWhiteSpace(log4NetConfig))
                log4NetConfig = "log4net.config";

            if (File.Exists(log4NetConfig))
            {
                XmlConfigurator.ConfigureAndWatch(new FileInfo(log4NetConfig));
            }
            else
            {
                XmlConfigurator.Configure(new MemoryStream(Encoding.UTF8.GetBytes(StaticConsoleAppender)));
            }
            _log = LogManager.GetLogger(typeof(Program));

            kafka4net.Logger.SetupLog4Net();

            var showHelp = false;
            string brokers = null;
            string topic = null;
            bool isProducer = false, isConsumer = false;
            int produceDelayMs = 1000;
            int totalMessages = -1;
            string file = null;

            var opts = new NDesk.Options.OptionSet
            {
                {"?|h|help", "Show Help", v => showHelp = true},
                {"p|producer", "Run Producer. Cannot be used with -c|-consumer.", v => isProducer = true },
                {"c|consumer", "Run Consumer. Cannot be used with -p|-producer.", v => isConsumer = true },
                {"d:|delay-ms:", "Producer delay between messages in ms (default 1000). Specify an integer number of milliseconds", (int v) => produceDelayMs = v },
                {"m:|messages:", "Total number of messages to produce or consume before closing. Specify -1 (default) to send/receive indefinitely.", (int v) => totalMessages = v },
                {"f:|file:", "File for producer to read from or consumer to write to. In producer mode, the file is read line by line, sending the content of each line as a message, and repeating as necessary. In Consumer mode, messages are written to this file line by line. If not specified, producer sends increasing numeric values, and consumer writes to console.", v => file = v },
                {"b=|brokers=", "Comman delimited list of Broker(s) to connect to. Can be just name/IP to use default port of 9092, or <name>:<port>", v => brokers = v },
                {"t=|topic=", "Topic to Produce or Consume from.", v => topic = v }
            };
            var unusedArgs = opts.Parse(args);
            if (unusedArgs.Any())
                ArgsFail($"Didn't recognize these command line options: {string.Join(", ", unusedArgs)}", opts);

            if (isProducer && isConsumer)
                ArgsFail("Cannot specify both producer and consumer",opts);

            if (!isProducer && !isConsumer)
                ArgsFail("Must specify either producer or consumer", opts);

            if (string.IsNullOrWhiteSpace(brokers))
                ArgsFail("Must specify one or more brokers to connect to.",opts);

            if (string.IsNullOrWhiteSpace(topic))
                ArgsFail("Must specify a topic name.",opts);

            if (!args.Any() || showHelp)
            {
                PrintHelp(opts);
                Environment.Exit(args.Any() ? 0 : 1);
            }

            if (isProducer)
            {
                var cancel = new CancellationTokenSource();
                var produceTask = ProducerExample.ProduceMessages(brokers, topic, produceDelayMs, totalMessages, file, cancel.Token);

                _log.Debug("Starting...");
                Task.WaitAny(produceTask, Task.Factory.StartNew(() =>
                {
                    Console.Out.WriteLine("Press 'Q' to cancel.");
                    var key = "";
                    while (key != "q")
                    {
                        key = Console.ReadKey().KeyChar.ToString().ToLower();
                    }

                    Console.WriteLine("Cancelling...");
                    cancel.Cancel();
                }, cancel.Token));

            }

            if (isConsumer)
            {
                var cancel = new CancellationTokenSource();
                var consumeTask = ConsumerExample.ConsumeMessages(brokers, topic, file, cancel.Token);

                _log.Debug("Starting...");
                Task.WaitAny(consumeTask, Task.Factory.StartNew(() =>
                {
                    Console.Out.WriteLine("Press 'Q' to cancel.");
                    var key = "";
                    while (key != "q")
                    {
                        key = Console.ReadKey().KeyChar.ToString().ToLower();
                    }

                    Console.WriteLine("Cancelling...");
                    cancel.Cancel();
                }, cancel.Token));

            }

        }

        private static void ArgsFail(string errMsg, NDesk.Options.OptionSet opts)
        {
            _log.Error(errMsg);
            Console.Error.WriteLine(errMsg);
            PrintHelp(opts);
            Environment.Exit(1);
        }

        private static void PrintHelp(NDesk.Options.OptionSet opts)
        {
            _log.InfoFormat("Showing options in console...");

            Console.Out.WriteLine($"Usage: examples (--producer|--consumer) --brokers <ip|name>[:port][,<ip|name>[:port]...] --topic=<topic-name> [Options]+");
            Console.Out.WriteLine("Options:");
            Console.Out.WriteLine();
            opts.WriteOptionDescriptions(Console.Out);
            Console.Out.WriteLine("Press any key to continue...");
            Console.In.Read();
        }
    }
}

using System;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace kafka4net
{
    public class Producer
    {
        private static readonly ILogger _log = Logger.GetLogger();

        public readonly ProducerConfiguration Configuration;
        public string Topic { get { return Configuration.Topic; } }
        public Cluster Cluster { get { return _cluster; } }

        public Action<Message[]> OnTempError;
        public Action<Exception, Message[]> OnPermError;
        public Action<Message[]> OnShutdownDirty;
        public Action<Message[]> OnSuccess;

        //public MessageCodec Codec = MessageCodec.CodecNone;
        private readonly TaskCompletionSource<bool> _completion = new TaskCompletionSource<bool>();
        private readonly BufferBlock<Message> _sendBuffer;
        private readonly Cluster _cluster;

        public Producer(ProducerConfiguration producerConfiguration)
        {
            Configuration = producerConfiguration;
            _cluster = new Cluster(producerConfiguration.SeedBrokers);
            _sendBuffer = new BufferBlock<Message>();

            // start up the subscription to the buffer and call SendBatchAsync on the Cluster when a batch is ready.
            _sendBuffer.AsObservable().
                Buffer(Configuration.BatchFlushTime, Configuration.BatchFlushSize).
                Where(b => b.Count > 0). // apparently, Buffer will trigger empty batches too, skip them
                ObserveOn(_cluster.Scheduler).  // Important! this needs to be AFTER Buffer call, because buffer execute on timer thread
                // and will ignore ObserveOn
                // TODO: how to check result? Make it failsafe?
                Subscribe(batch => _cluster.SendBatchAsync(this, batch), // TODO: how to check result? Make it failsafe?
                // How to prevent overlap?
                // Make sure producer.OnError are fired
                    e =>
                    {
                        _log.Fatal("Unexpected error in send buffer: {0}", e.Message);
                        _completion.TrySetResult(false);
                    },
                    () =>
                    {
                        _log.Info("Send buffer complete");
                        _completion.TrySetResult(true);
                    });
        }

        public Task ConnectAsync()
        {
            return _cluster.ConnectAsync();
        }

        public void Send(Message msg)
        {
            _sendBuffer.Post(msg);
        }

        public async Task Close(TimeSpan timeout)
        {
            _log.Debug("Closing...");
            _sendBuffer.Complete();
            
            //await _sendBuffer.Completion; // apparently it wont wait till sequence is complete
            await _completion.Task.ConfigureAwait(false);

            await _cluster.CloseAsync(timeout);
            _log.Info("Close complete");
        }

        public override string ToString()
        {
            return string.Format("'{0}' Batch flush time: {1} Batch flush size: {2}", Topic, Configuration.BatchFlushTime, Configuration.BatchFlushSize);
        }
    }
}

using System;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace kafka4net
{
    public class Publisher
    {
        internal readonly string Topic;
        private BufferBlock<Message> _sendBuffer;
        public short Acks = 1;
        public int TimeoutMs = 1000;
        public TimeSpan BatchTime = TimeSpan.FromMilliseconds(500);
        public int BatchSize = 1000;
        //public MessageCodec Codec = MessageCodec.CodecNone;
        static readonly ILogger _log = Logger.GetLogger();
        readonly TaskCompletionSource<bool> _completion = new TaskCompletionSource<bool>();

        public Action<Message[]> OnTempError;
        public Action<Exception, Message[]> OnPermError;
        public Action<Message[]> OnShutdownDirty;
        public Action<Message[]> OnSuccess;

        public Publisher(string topic)
        {
            Topic = topic;
        }

        public void Connect(Router router)
        {
            _sendBuffer = new BufferBlock<Message>();
            _sendBuffer.AsObservable().
                Buffer(BatchTime, BatchSize).
                Where(b => b.Count > 0). // apparently, Buffer will trigger empty batches too, skip them
                //Select(async batch => await router.SendBatch(this, batch)). // TODO: how to check result? Make it failsafe?
                //Subscribe(_ => _log.Debug("send batch status:", (Exception)_.Exception),
                //// How to prevent overlap?
                //// Make sure publisher.OnError are fired
                //e => _log.Fatal("Unexpected error in send buffer: {0}", e.Message),
                //() => _log.Info("Send buffer complete"));
                // TODO: how to check result? Make it failsafe?
                Subscribe(batch => router.SendBatch(this, batch), // TODO: how to check result? Make it failsafe?
                // How to prevent overlap?
                // Make sure publisher.OnError are fired
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

        public void Send(Message msg)
        {
            _sendBuffer.Post(msg);
        }

        public async Task Close()
        {
            _log.Debug("Closing...");
            _sendBuffer.Complete();
            
            //await _sendBuffer.Completion; // apparently it wont wait till sequence is complete
            await _completion.Task;
            
            _log.Info("Close complete");
        }

        public override string ToString()
        {
            return string.Format("'{0}' Batch time: {1} Batch Size: {2}", Topic, BatchTime, BatchSize);
        }
    }
}

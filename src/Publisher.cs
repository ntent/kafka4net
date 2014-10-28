using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net.Utils;

namespace kafka4net
{
    public class Publisher
    {
        internal readonly string Topic;
        public short Acks = 1;
        public int TimeoutMs = 1000;
        public TimeSpan BatchTime = TimeSpan.FromMilliseconds(500);
        public int BatchSize = 1000;

        public Action<Message[]> OnTempError;
        public Action<Exception, Message[]> OnPermError;
        public Action<Message[]> OnShutdownDirty;
        public Action<Message[]> OnSuccess;

        //public MessageCodec Codec = MessageCodec.CodecNone;
        readonly TaskCompletionSource<bool> _completion = new TaskCompletionSource<bool>();
        private BufferBlock<Message> _sendBuffer;
        static readonly ILogger _log = Logger.GetLogger();
        private EventLoopScheduler _scheduler;

        public Publisher(string topic)
        {
            Topic = topic;
        }

        public void Connect(Router router)
        {
            _scheduler = router.Scheduler;
            _sendBuffer = new BufferBlock<Message>();

            _sendBuffer.AsObservable().
                Buffer(BatchTime, BatchSize).
                Where(b => b.Count > 0). // apparently, Buffer will trigger empty batches too, skip them
                ObserveOn(_scheduler).  // Important! this needs to be AFTER Buffer call, because buffer execute on timer thread
                                        // and will ignore ObserveOn
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
            await _completion.Task.ConfigureAwait(false);
            
            _log.Info("Close complete");
        }

        public override string ToString()
        {
            return string.Format("'{0}' Batch time: {1} Batch Size: {2}", Topic, BatchTime, BatchSize);
        }
    }
}

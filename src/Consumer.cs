using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer
    {
        internal string Topic { get; private set; }
        internal int MaxWaitTimeMs { get; private set; }
        internal int MinBytes { get; private set; }
        internal int MaxBytes { get; private set; }
        
        readonly Router _router;
        internal readonly long Time;
        readonly Subject<ReceivedMessage> _events = new Subject<ReceivedMessage>();
        public IObservable<ReceivedMessage> AsObservable { get { return _events; } }
        static readonly ILogger _log = Logger.GetLogger();

        // TODO: API for [part,offset]
        public Consumer(string topic, Router router, int maxWaitTimeMs=500, int minBytes=1, long offset=-1L, int maxBytes=256*1024)
        {
            _router = router;
            Time = offset;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytes = minBytes;
            MaxBytes = maxBytes;
            
            // TODO: bug: Consumer's subscriber is not subscribed yet and 
            // may loss some messages. Subscribe explicitly.
            Subscribe();
        }

        private async void Subscribe()
        {
            // TODO: if caller sets SynchronizationContext and it is blocked, fetcher cretion is delayed until caller
            // unblocks. For example, NUnit's async void test method.
            (await _router.InitFetching(this)).
                Subscribe(msg => _events.OnNext(msg));
        }

        public void Unsubscribe() {
            // TODO:
        }

        public override string ToString()
        {
            return string.Format("Topic: '{0}', offset: {1}", Topic, Time);
        }

        internal void OnPartitionsRecovered(IObservable<FetchResponse.TopicFetchData> recoveredPartitions)
        {
            recoveredPartitions.
                Subscribe(fetch => (
                    from part in fetch.Partitions
                    from msg in part.Messages
                    select new ReceivedMessage
                    {
                        Topic = fetch.Topic,
                        Partition = part.Partition,
                        Key = msg.Key,
                        Value = msg.Value
                    }).
                    ForEach(msg => _events.OnNext(msg))
                );
        }
    }
}

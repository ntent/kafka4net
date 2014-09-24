using System;
using System.Linq;
using System.Reactive.Subjects;
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

        public Consumer(string topic, Router router, int maxWaitTimeMs, int minBytes, long offset, int maxBytes)
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

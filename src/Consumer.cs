using System;
using System.Linq;
using System.Reactive.Subjects;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer : IObserver<ReceivedMessage>
    {
        internal string Topic { get; private set; }
        internal int MaxWaitTimeMs { get; private set; }
        internal int MinBytes { get; private set; }
        internal int MaxBytes { get; private set; }
        
        readonly Router _router;
        internal readonly long Time;
        readonly Subject<ReceivedMessage> _events = new Subject<ReceivedMessage>();
        public IObservable<ReceivedMessage> AsObservable { get { return _events; } }

        // with new organization, use a Subject to connect to all TopicPartitions
        private Subject<ReceivedMessage> _receivedMessageSubject = new Subject<ReceivedMessage>();
 
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

        /// <summary>
        /// Gets the Observer side of this Consumer that can observe from many TopicPartitions
        /// </summary>
        public IObserver<ReceivedMessage> MessageObserver { get { return _receivedMessageSubject; } }

        public void OnNext(ReceivedMessage value)
        {
            // check that we have someone subscribed to the _receivedMessageSubject otherwise we could lose messages!
            if (!_receivedMessageSubject.HasObservers)
                _log.Error("Got message from TopicPartition with no Observers!");

            // go ahead and pass it along anyway
            _receivedMessageSubject.OnNext(value);
        }

        public void OnError(Exception error)
        {
            _log.Error("Exception sent from TopicPartition!",error);

            // go ahead and pass it along
            _receivedMessageSubject.OnError(error);
        }

        /// <summary>
        /// This happens when an underlying TopicPartition calls OnCompleted... why would this occur?! Should not, other than when closing the whole consumer?
        /// </summary>
        public void OnCompleted()
        {
            _log.Info("Completed receiving from TopicPartition");

            // go ahead and pass it along
            _receivedMessageSubject.OnCompleted();
        }
    }
}

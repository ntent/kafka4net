using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer : IObserver<ReceivedMessage>
    {
        internal string Topic { get; private set; }
        internal int MaxWaitTimeMs { get; private set; }
        internal int MinBytes { get; private set; }
        internal int MaxBytes { get; private set; }
        
        Router _router;
        internal readonly Func<int, long> PartitionOffsetProvider;
        internal readonly bool StartFromQueueHead;
        readonly Subject<ReceivedMessage> _events = new Subject<ReceivedMessage>();
        public IObservable<ReceivedMessage> AsObservable { get { return _events; } }

        // with new organization, use a Subject to connect to all TopicPartitions
        private Subject<ReceivedMessage> _receivedMessageSubject = new Subject<ReceivedMessage>();
 
        static readonly ILogger _log = Logger.GetLogger();


        /// <summary>
        /// Subscription is performed asynchronously.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="router"></param>
        /// <param name="maxWaitTimeMs"></param>
        /// <param name="minBytes"></param>
        /// <param name="partitionOffsetProvider">
        ///     If not provided or null, all partitions are positioned at the end (only new messages will be picked up)
        /// </param>
        /// <param name="maxBytes"></param>
        /// <param name="startFromQueueHead">Start reading messages from the head of the queue</param>
        public Consumer(string topic, int maxWaitTimeMs=500, int minBytes=1, Func<int,long> partitionOffsetProvider = null, int maxBytes=256*1024, bool startFromQueueHead=false)
        {
            if(startFromQueueHead && partitionOffsetProvider != null)
                throw new ArgumentException("partitionOffsetProvider and startFromQueueHead can not be both set");

            PartitionOffsetProvider = partitionOffsetProvider;
            StartFromQueueHead = startFromQueueHead;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytes = minBytes;
            MaxBytes = maxBytes;
        }

        public async Task Subscribe(Router router)
        {
            // TODO: if caller sets SynchronizationContext and it is blocked, fetcher cretion is delayed until caller
            // unblocks. For example, NUnit's async void test method.
            _router = router;
            var fetch = await _router.InitFetching(this).ConfigureAwait(false);
            await _router.Scheduler.Ask(() => fetch.Subscribe(msg => _events.OnNext(msg)));
        }

        public void Unsubscribe() {
            // TODO:
        }

        public override string ToString()
        {
            return string.Format("Consumer: '{0}'", Topic);
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

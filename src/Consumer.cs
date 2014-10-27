using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.ConsumerImpl;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer : ISubject<ReceivedMessage>, IDisposable
    {
        private static readonly ILogger _log = Logger.GetLogger();

        internal ConsumerConfiguration Configuration { get; private set; }
        internal string Topic { get { return Configuration.Topic; } }

        private readonly Router _router;


        readonly Subject<ReceivedMessage> _events = new Subject<ReceivedMessage>();

        // with new organization, use a Subject to connect to all TopicPartitions
        private readonly Subject<ReceivedMessage> _receivedMessageStream = new Subject<ReceivedMessage>();
 

        /// <summary>
        /// Create a new consumer using the specified configuration. See @ConsumerConfiguration
        /// </summary>
        /// <param name="consumerConfig"></param>
        public Consumer(ConsumerConfiguration consumerConfig)
        {
            Configuration = consumerConfig;
            _router = new Router(consumerConfig.SeedBrokers);
        }

        /// <summary>
        /// Pass connection method through from Router
        /// </summary>
        /// <returns></returns>
        public Task ConnectAsync()
        {
            return _router.ConnectAsync();
        }

        public Task CloseAsync(TimeSpan timeout)
        {
            return _router.Close(timeout);
        }

        public IDisposable Subscribe(IObserver<ReceivedMessage> observer)
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Consumer is already disposed.");

            if (_router.State != Router.BrokerState.Connected)
                throw new Exception("Must connect the consumer by calling ConnectAsync before consuming.");

            // subscribe the observer to the ReceivedMessageStream
            var subscription = _receivedMessageStream.Subscribe(observer);

            // TODO: Get and subscribe to all TopicPartitions 

            // TODO: Wrap subscriptions into CompositDisposable, dispose when consumer subscription is disposed.
            return subscription;
        }

        public override string ToString()
        {
            return string.Format("Consumer: '{0}'", Topic);
        }

        private bool _isDisposed;
        public void Dispose()
        {
            if (_isDisposed)
                return;

            // close and release the connections in the Router.
            if (_router != null && _router.State != Router.BrokerState.Disconnected)
                _router.Close(TimeSpan.FromSeconds(5)).Wait();

            _isDisposed = true;
        }

        /// <summary>
        /// TODO: Remove this!
        /// </summary>
        /// <param name="recoveredPartitions"></param>
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

        public void OnNext(ReceivedMessage value)
        {
            // check that we have someone subscribed to the _receivedMessageSubject otherwise we could lose messages!
            if (!_receivedMessageStream.HasObservers)
                _log.Error("Got message from TopicPartition with no Observers!");

            // go ahead and pass it along anyway
            _receivedMessageStream.OnNext(value);
        }

        public void OnError(Exception error)
        {
            _log.Error("Exception sent from TopicPartition!",error);

            // go ahead and pass it along
            _receivedMessageStream.OnError(error);
        }

        /// <summary>
        /// This happens when an underlying TopicPartition calls OnCompleted... why would this occur?! Should not, other than when closing the whole consumer?
        /// </summary>
        public void OnCompleted()
        {
            _log.Info("Completed receiving from TopicPartition");

            // go ahead and pass it along
            _receivedMessageStream.OnCompleted();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
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
        private Task _sendLoopTask;
        private readonly Subject<Message> _sendMessagesSubject;
        private readonly Cluster _cluster;

        // cancellation token used to notify all producer components to stop.
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        // the allPartitionQueues dictionary contains queues for all of the "PartitionBatches" indexed by topic/partition. 
        // the sending loop will send out of these queues as quickly as possible, however when there is an issue with 
        // sending, these queues will back up (potentially on only certain partitions)
        // TODO: the Queue in here needs to eventually be wrapped with logic to have size bounds
        private readonly Dictionary<int, Queue<Message[]>> _allPartitionQueues = new Dictionary<int, Queue<Message[]>>();

        public Producer(ProducerConfiguration producerConfiguration)
        {
            Configuration = producerConfiguration;
            _cluster = new Cluster(producerConfiguration.SeedBrokers);
            _sendMessagesSubject = new Subject<Message>();
        }

        public async Task ConnectAsync()
        {
            await _cluster.ConnectAsync();


            // Buffered Observable Notifies on "release" of a batch to send.
            var bufferedBatches = _sendMessagesSubject
                .AsObservable()
                .Buffer(TimeSpan.FromSeconds(5), 10)
                .Do(_ => { /* TODO: Notify of batch delivered */});

            // get all of the partitions for this topic. Allows the MessagePartitioner to select a partition.
            var topicPartitions = await _cluster.GetOrFetchMetaForTopicAsync(Configuration.Topic);

            // topic partition batches takes a single batch, flattens it back out, and groups by topic and partition.
            // this observable broadcasts "TopicPartitionMessageBatches" into the send buffer which then is pull-based for the sender loop.
            var topicPartitionBatches = bufferedBatches
                .Select(batchMessages => batchMessages.GroupBy(m => Configuration.Partitioner.GetMessagePartition(m, topicPartitions)));


            // this subscription takes the incoming stream of sent messages and adds them to the appropriate queue for sending.
            topicPartitionBatches.Subscribe(partitionGroupedBatches =>
            {
                foreach (var partitionBatch in partitionGroupedBatches)
                {
                    var key = partitionBatch.Key.Id;

                    // first check and add a queue if necessary
                    if (!_allPartitionQueues.ContainsKey(key))
                    {
                        _allPartitionQueues.Add(key, new Queue<Message[]>());
                    }

                    var queue = _allPartitionQueues[key];

                    // dump to the queue.
                    queue.Enqueue(partitionBatch.ToArray());
                }

            });

            // start the send loop task
            _sendLoopTask = SendLoop();

            // TODO: Old logic. remove once new logic is in place.
            // start up the subscription to the buffer and call SendBatchAsync on the Cluster when a batch is ready.
            //_sendMessagesSubject.AsObservable().
            //    Buffer(Configuration.BatchFlushTime, Configuration.BatchFlushSize).
            //    Where(b => b.Count > 0). // apparently, Buffer will trigger empty batches too, skip them
            //    ObserveOn(_cluster.Scheduler).  // Important! this needs to be AFTER Buffer call, because buffer execute on timer thread
            //    // and will ignore ObserveOn
            //    // TODO: how to check result? Make it failsafe?
            //    Subscribe(batch => _cluster.SendBatchAsync(this, batch), // TODO: how to check result? Make it failsafe?
            //    // How to prevent overlap?
            //    // Make sure producer.OnError are fired
            //        e =>
            //        {
            //            _log.Fatal("Unexpected error in send buffer: {0}", e.Message);
            //            _completion.TrySetResult(false);
            //        },
            //        () =>
            //        {
            //            _log.Info("Send buffer complete");
            //            _completion.TrySetResult(true);
            //        });

        }

        public void Send(Message msg)
        {
            if (_cancellation.IsCancellationRequested)
                throw new Exception("Cannot send messages after producer is canceled / closed.");
                
            _sendMessagesSubject.OnNext(msg);

        }

        public async Task Close(TimeSpan timeout)
        {
            _log.Debug("Closing...");
            // mark the cancellation token to cause the sending to finish up and don't allow any new messages coming in.
            _cancellation.Cancel();

            // complete the incoming message stream
            _sendMessagesSubject.OnCompleted();
            
            // wait for sending to complete 
            await _sendLoopTask.ConfigureAwait(false);

            // close down the cluster
            await _cluster.CloseAsync(timeout);
            _log.Info("Close complete");
        }

        private async Task SendLoop()
        {

            while (true)
            {
                // get a list containing the first batch in each queue to be sent.
                var outgoingBatch = new List<Tuple<int, Message[]>>();

                foreach (var kvp in _allPartitionQueues)
                {
                    if (kvp.Value.Count > 0)
                    {
                        // here we just PEEK as we don't want it to be taken from the queue until we successfully send the batch.
                        outgoingBatch.Add(new Tuple<int, Message[]>(kvp.Key, kvp.Value.Peek()));
                    }
                }

                // now we have a batch outgoing, send and if successful, dequeue
                Console.WriteLine("outgoing batch has: {0}", string.Join(",", outgoingBatch.Select(b => string.Format("p-{0}:{1}msgs", b.Item1, b.Item2.Length))));


                // TODO: Do Send asynchronously here for all brokers and handle result.
                // look up broker for each
                // then Group by Broker
                // then call SendAsync on all at the same time, awaiting the results of all of them
                // process results as they come back

                // NOTE: Pseudo-code from linqpad sample on handling results:
                //if (rnd.NextDouble() > 0.2)
                //{
                //    // simulate success
                //    Console.WriteLine("Success, removing batches");
                //    foreach (var batch in outgoingBatch)
                //    {
                //        var key = new Tuple<string, int>(batch.Item1, batch.Item2);
                //        var removed = allTopicPartitionQueues[key].Dequeue();
                //        Console.WriteLine("removed batch of {0} messages from {1}", removed.Length, key);
                //    }
                //}
                //else
                //{
                //    Console.WriteLine("Failed, do not remove batch.");
                //}
            }            
        }

        public override string ToString()
        {
            return string.Format("'{0}' Batch flush time: {1} Batch flush size: {2}", Topic, Configuration.BatchFlushTime, Configuration.BatchFlushSize);
        }
    }
}

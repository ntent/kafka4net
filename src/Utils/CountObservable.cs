using System;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;

namespace kafka4net.Utils
{
    public sealed class CountObservable : IObservable<int>
    {
        readonly ISubject<int> _subj;
        readonly IConnectableObservable<int> _publishedCounter;
        int _count;

        public CountObservable()
        {
            _subj = new Subject<int>();
            _publishedCounter = _subj.Replay(1);
            _publishedCounter.Connect();
            _subj.OnNext(0);
        }

        public void Incr() 
        {
            _subj.OnNext(Interlocked.Increment(ref _count));
        }

        public void Decr()
        {
            _subj.OnNext(Interlocked.Decrement(ref _count));
        }

        public IDisposable Subscribe(IObserver<int> observer)
        {
            return _publishedCounter.Subscribe(observer);
        }
    }
}

using System;
using System.Diagnostics;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace kafka4net.Utils
{
    /// <summary>
    /// Wrapper around scheduler, which check execution duration every second and expose the duration as DelaySampler observable.
    /// The caller can use DelaySampler to set treshold and take an action if it is suspected that working thread is hang.
    /// Also it capture the last executed stack, which help finding offending code.
    /// </summary>
    class WatchdogScheduler : LocalScheduler, ISchedulerPeriodic, IDisposable
    {
        readonly IScheduler _scheduler;
        DateTime _actionStart = DateTime.MaxValue;
        public readonly IObservable<TimeSpan> DelaySampler;
        public StackTrace LastStack;
        bool _disposed;
        private static readonly ILogger _log = Logger.GetLogger();


        public WatchdogScheduler(IScheduler scheduler)
        {
            _scheduler = scheduler;

            DelaySampler = Observable.Interval(TimeSpan.FromSeconds(1)).
                Select(_ => _actionStart).
                Where(_ => _ != DateTime.MaxValue).
                Select(_ => DateTime.Now - _);
        }

        public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action)
        {
            var stack = new StackTrace(false);
            Func<IScheduler, TState, IDisposable> actionWrapper = (scheduler, state1) => 
            {
                _actionStart = DateTime.Now;
                LastStack = stack;
                try
                {
                    return action(scheduler, state1);
                }
                finally
                {
                    LastStack = null;
                    _actionStart = DateTime.MaxValue;
                }
            };
            
            return _scheduler.Schedule(state, dueTime, actionWrapper);
        }

        public IDisposable SchedulePeriodic<TState>(TState state, TimeSpan period, Func<TState, TState> action)
        {
            var stack = new StackTrace(false);
            Func<TState, TState> actionWrapper = (s1) =>
            {
                _actionStart = DateTime.Now;
                LastStack = stack;
                try
                {
                    return action(s1);
                }
                finally
                {
                    LastStack = null;
                    _actionStart = DateTime.MaxValue;
                }
            };

            return _scheduler.SchedulePeriodic(state, period, actionWrapper);
        }

        public void Dispose()
        {
            if(_disposed)
                return;
            _disposed = true;

            (_scheduler as IDisposable)?.Dispose();
        }
    }
}
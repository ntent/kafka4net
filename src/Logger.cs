using System;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace kafka4net
{
    public class Logger
    {
        static Func<Type, ILogger> LoggerFactory = t => new NullLogger();

        public static void SetupNLog()
        {
            var logManagerType = Type.GetType("NLog.LogManager, NLog");
            var method = logManagerType.GetMethod("GetLogger", BindingFlags.InvokeMethod | BindingFlags.Public | BindingFlags.Static, null, CallingConventions.Standard, new[] {typeof(string)}, null);

            var retType = method.ReturnType;
            var delegateTypeGeneric = typeof(Func<,>);
            var delegateType = delegateTypeGeneric.MakeGenericType(new[] {typeof(string), retType});
            var del = Delegate.CreateDelegate(delegateType, method);
            var func = (Func<string,object>)del;
            LoggerFactory = t => new NLogAdaprter(func(t.FullName));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static ILogger GetLogger()
        {
            var frame = new StackFrame(1, false);
            var klass = frame.GetMethod().DeclaringType;
            return LoggerFactory(klass);
        }

        class NullLogger : ILogger
        {
            public void Debug(string msg) { }
            public void Debug(Exception e, string msg, params object[] args) {}
            public void Debug(string msg, params object[] args) { }
            public void Info(string msg) {}
            public void Info(string msg, params object[] args) {}
            public void Info(Exception e, string msg, params object[] args) {}
            public void Error(string msg) {}
            public void Error(string msg, params object[] args) {}
            public void Error(Exception e, string msg, params object[] args) {}
            public void Fatal(string msg) {}
            public void Fatal(string msg, params object[] args) {}
            public void Fatal(Exception e, string msg, params object[] args) {}
            public bool IsDebugEnabled { get { return false; } }
        }

        class NLogAdaprter : ILogger 
        {
            readonly dynamic _actualLogger;

            public NLogAdaprter(object actualLogger)
            {
                _actualLogger = actualLogger;
            }

            public void Debug(string msg)
            {
                _actualLogger.Debug(msg);
            }

            public void Debug(string msg, params object[] args)
            {
                _actualLogger.Debug(msg, args);
            }

            public void Debug(Exception e, string msg, params object[] args)
            {
                _actualLogger.Debug(string.Format(msg, args), e);
            }

            public bool IsDebugEnabled { get { return _actualLogger.IsDebugEnabled; } }

            public void Info(string msg)
            {
                _actualLogger.Info(msg);
            }

            public void Info(string msg, params object[] args)
            {
                _actualLogger.Info(msg, args);
            }

            public void Info(Exception e, string msg, params object[] args)
            {
                _actualLogger.Info(string.Format(msg, args), e);
            }

            public void Error(string msg)
            {
                _actualLogger.Error(msg);
            }

            public void Error(string msg, params object[] args)
            {
                _actualLogger.Error(msg, args);
            }

            public void Error(Exception e, string msg, params object[] args)
            {
                _actualLogger.Error(string.Format(msg, args), e);
            }

            public void Fatal(string msg)
            {
                _actualLogger.Fatal(msg);
            }

            public void Fatal(string msg, params object[] args)
            {
                _actualLogger.Fatal(msg, args);
            }

            public void Fatal(Exception e, string msg, params object[] args)
            {
                _actualLogger.Fatal(string.Format(msg, args), e);
            }
        }
    }
}

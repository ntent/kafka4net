using System;

namespace kafka4net
{
    internal interface ILogger
    {
        void Debug(string msg);
        void Debug(string msg, params object[] args);
        void Debug(Exception e, string msg, params object[] args);
        void Info(string msg);
        void Info(string msg, params object[] args);
        void Info(Exception e, string msg, params object[] args);
        void Error(string msg);
        void Error(string msg, params object[] args);
        void Error(Exception e, string msg, params object[] args);
        void Fatal(string msg);
        void Fatal(string msg, params object[] args);
        void Fatal(Exception e, string msg, params object[] args);
        bool IsDebugEnabled { get; }
    }
}

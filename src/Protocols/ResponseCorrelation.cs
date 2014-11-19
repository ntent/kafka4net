using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    class ResponseCorrelation
    {
        // It is possible to have id per connection, but it's just simpler code and debug/tracing when it's global
        private static int _correlationId;
        private readonly ConcurrentDictionary<int, Action<byte[], Exception>> _corelationTable = new ConcurrentDictionary<int, Action<byte[], Exception>>();
        private readonly Action<Exception> _onError;
        private static readonly ILogger _log = Logger.GetLogger();
        readonly string _id;

        public ResponseCorrelation(Action<Exception> onError, string id = "")
        {
            _onError = onError;
            _id = id;
        }

        internal async Task CorrelateResponseLoop(TcpClient client, CancellationToken cancel)
        {
            try
            {
                _log.Debug("Starting reading loop from socket");
                // TODO: if corrup message, dump bin log of 100 bytes for further investigation
                while (client.Connected)
                {
                    try
                    {
                        // read message size
                        var buff = new byte[4];
                        var read = await client.GetStream().ReadAsync(buff, 0, 4, cancel);
                        if (cancel.IsCancellationRequested)
                        {
                            _log.Debug("Stopped reading from {0} because socket cancelled", client.Client.RemoteEndPoint);
                            return;
                        }

                        // TODO: what to do if read<4 ? Recycle connection?
                        if (read == 0)
                        {
                            _log.Info("Server closed connection");
                            client.Close();
                            return;
                        }
                        var size = BigEndianConverter.ToInt32(buff);
                        // read message body
                        var body = new byte[size];
                        var pos = 0;
                        var left = size;
                        do
                        {
                            read = await client.GetStream().ReadAsync(body, pos, left);
                            if (read == 0)
                            {
                                _log.Info("Server closed connection");
                                client.Close();
                                return;
                            }
                            pos += read;
                            left -= read;
                        } while (left > 0);

                        try
                        {
                            int correlationId = -1;
                            // TODO: check read==size && read > 4
                            correlationId = BigEndianConverter.ToInt32(body);
                            //_log.Debug("<-{0}\n {1}", correlationId, FormatBytes(body));
                            //_log.Debug("<-{0}", correlationId);

                            // find correlated action
                            Action<byte[], Exception> handler;
                            // TODO: if correlation id is not found, there is a chance of corrupt 
                            // connection. Maybe recycle the connection?
                            if (!_corelationTable.TryRemove(correlationId, out handler))
                            {
                                _log.Error("Unknown correlationId: " + correlationId);
                                continue;
                            }
                            handler(body, null);
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex, "Error with handling message. Message bytes:\r\n{0}", FormatBytes(body));
                            throw;
                        }
                    }
                    catch (SocketException e)
                    {
                        // shorter version of socket exception, without stack trace dump
                        _log.Info("CorrelationLoop socket exception. {0}", e.Message);
                        throw;
                    }
                    catch (ObjectDisposedException)
                    {
                        _log.Info("CorrelationLoop socket exception. Object disposed");
                        throw;
                    }
                    catch (Exception e)
                    {
                        _log.Error(e, "CorrelateResponseLoop error");
                        if (_onError != null)
                            _onError(e);
                        throw;
                    }
                }

                _log.Debug("Finished reading loop from socket");
            }
            catch (Exception e)
            {
                _corelationTable.Values.ForEach(c => c(null, e));
                if (_onError != null)
                    _onError(e);
                throw;
            }
            finally
            {
                _corelationTable.Values.ForEach(c => c(null, new CorrelationLoopException("Correlation loop closed. Request will never get a response.")));
            }
        }

        internal async Task<T> SendAndCorrelateAsync<T>(Func<int, byte[]> serialize, Func<byte[], T> deserialize, TcpClient tcp, CancellationToken cancel)
        {
            var correlationId = Interlocked.Increment(ref _correlationId);

            var callback = new TaskCompletionSource<T>();
            // TODO: configurable timeout
            // TODO: coordinate with Message's timeout
            // TODO: set exception in case of tcp socket exception and end of correlation loop
            //var timeout = new CancellationTokenSource(5 * 1000);
            try
            {
                var res = _corelationTable.TryAdd(correlationId, (body, ex) =>
                {
                    if (ex == null)
                        callback.TrySetResult(deserialize(body));
                    else
                        callback.TrySetException(ex);
                    //timeout.Dispose();
                });
                if (!res)
                    throw new ApplicationException("Failed to add correlationId: " + correlationId);
                var buff = serialize(correlationId);
                //_log.Debug("{0}->\n{1}", correlationId, FormatBytes(buff));
                //_log.Debug("{0}->", correlationId);
                await tcp.GetStream().WriteAsync(buff, 0, buff.Length, cancel);
            }
            catch (Exception e)
            {
                callback.TrySetException(e);
                if (_onError != null)
                    _onError(e);
                throw;
            }

            // TODO: is stream safe to use after timeout?
            cancel.Register(() => callback.TrySetCanceled(), useSynchronizationContext: false);
            try
            {
                return await callback.Task;
            }
            catch (TaskCanceledException e)
            {
                if (_onError != null)
                    _onError(e);
                throw;
            }
        }

        private static string FormatBytes(byte[] buff)
        {
            return buff.Aggregate(new StringBuilder(), (builder, b) => builder.Append(b.ToString("x2")),
                str => str.ToString());
        }
    }
}

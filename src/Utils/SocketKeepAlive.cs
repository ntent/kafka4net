using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.Utils
{

    /// <summary>
    /// Socket class extensions to set SetKeepAliveValues.
    /// </summary>
    public static class SocketKeepAlive
    {

        // Convert tcp_keepalive C struct To C# struct
        [
            System.Runtime.InteropServices.StructLayout
            (
                System.Runtime.InteropServices.LayoutKind.Explicit
            )
        ]
        unsafe struct TcpKeepAlive
        {
            [System.Runtime.InteropServices.FieldOffset(0)]
            [
                System.Runtime.InteropServices.MarshalAs
                (
                    System.Runtime.InteropServices.UnmanagedType.ByValArray,
                    SizeConst = 12
                )
            ]
            public fixed byte Bytes[12];

            [System.Runtime.InteropServices.FieldOffset(0)]
            public uint On_Off;

            [System.Runtime.InteropServices.FieldOffset(4)]
            public uint KeepaLiveTime;

            [System.Runtime.InteropServices.FieldOffset(8)]
            public uint KeepaLiveInterval;
        }

        public static void SetKeepAliveValues(this Socket socket, bool useKeepAlive, uint keepAliveTimeMs, uint keepAliveIntervalMs)
        {
            unsafe
            {
                TcpKeepAlive keepAliveValues = new TcpKeepAlive();

                keepAliveValues.On_Off = Convert.ToUInt32(useKeepAlive);
                keepAliveValues.KeepaLiveTime = keepAliveTimeMs;
                keepAliveValues.KeepaLiveInterval = keepAliveIntervalMs;

                byte[] inValue = new byte[12];

                for (int I = 0; I < 12; I++)
                    inValue[I] = keepAliveValues.Bytes[I];

                socket.IOControl(IOControlCode.KeepAliveValues, inValue, null);
            }

        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace TemplateJobWorkerNode.Utilities
{
    static class DisableConsoleQuickEdit
    {
        const uint ENABLE_QUICK_EDIT = 0x1234;
        const int STD_INPUT_HANDLE = -10; // Standard input device

        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("kernel32.dll")]
        static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint lpMode);

        [DllImport("kernel32.dll")]
        static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint dwMode);

        internal static bool DisableQuickEdit()
        {
            IntPtr consoleHandle = GetStdHandle(STD_INPUT_HANDLE);
            if (!GetConsoleMode(consoleHandle, out uint consoleMode))
                return false;

            // Clear the quick edit bit in the mode flags
            consoleMode &= ~ENABLE_QUICK_EDIT;

            if (!SetConsoleMode(consoleHandle, consoleMode))
                return false;

            return true;
        }
    }
}

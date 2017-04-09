using log4net;
using log4net.Config;

namespace Kontur.GameStats.Server.Utils
{
    public static class Logger
    {
        private static ILog log = LogManager.GetLogger("LOGGER");

        public static ILog Log => log;

        public static void InitLogger()
        {
            XmlConfigurator.Configure();
        }
    }
}

using System;
using System.Collections.Generic;
using Fclp;
using Kontur.GameStats.Server.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Kontur.GameStats.Server
{
    public class EntryPoint
    {
        public static void Main(string[] args)
        {
            JsonConvert_Init();

            var commandLineParser = new FluentCommandLineParser<Options>();

            commandLineParser
                .Setup(options => options.Prefix)
                .As("prefix")
                .SetDefault("http://+:8080/")
                .WithDescription("HTTP prefix to listen on");

            commandLineParser
                .SetupHelp("h", "help")
                .WithHeader($"{AppDomain.CurrentDomain.FriendlyName} [--prefix <prefix>]")
                .Callback(text => Console.WriteLine(text));

            if (commandLineParser.Parse(args).HelpCalled)
                return;

            RunServer(commandLineParser.Object);
        }

        private static void RunServer(Options options)
        {
            using(var server = new StatServer())
            {
                server.Start(options.Prefix);

                Console.ReadKey(true);
            }
        }

        public static void JsonConvert_Init()
        {
            JsonConvert.DefaultSettings =
                () => new JsonSerializerSettings
                      {
                          ContractResolver = new CamelCasePropertyNamesContractResolver(),
                          FloatParseHandling = FloatParseHandling.Decimal,
                          DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                          Converters = new List<JsonConverter>
                                       {
                                           new DecimalFormatJsonConverter()
                                       }
                      };
        }

        private class Options
        {
            public string Prefix { get; set; }
        }
    }
}

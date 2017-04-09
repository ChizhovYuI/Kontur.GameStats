using System;
using System.Collections.Generic;
using Kontur.GameStats.Server.Domains;

namespace Kontur.GameStats.Server.Tests.Utils
{
    public static class ExampleDomains
    {
        public static readonly ServerInfo ServerInfo = new ServerInfo("] My P3rfect Server [", new[] {"DM", "TDM"});

        public const string SerilizedServerInfo =
            "{" +
            "\"name\":\"] My P3rfect Server [\"," +
            "\"gameModes\":[\"DM\",\"TDM\"]" +
            "}";

        public static readonly Domains.Server Server = new Domains.Server("167.42.23.32-1337", ServerInfo);

        public const string SerializedServer =
            "{" +
            "\"endpoint\":\"167.42.23.32-1337\"," +
            "\"info\":" +
            SerilizedServerInfo +
            "}";

        public static readonly MatchResult MatchResult = new MatchResult(
            "DM-HelloWorld",
            "DM",
            20,
            20,
            (decimal)12.345678,
            new List<Scoreboard> {new Scoreboard("Player1", 20, 21, 3), new Scoreboard("Player2", 2, 2, 21)});

        public const string SerialaizedMatchResult =
            "{" +
            "\"map\":\"DM-HelloWorld\",\"gameMode\":\"DM\"," +
            "\"fragLimit\":20," +
            "\"timeLimit\":20," +
            "\"timeElapsed\":12.345678," +
            "\"scoreboard\":[" +
            "{" +
            "\"name\":\"Player1\"," +
            "\"frags\":20," +
            "\"kills\":21," +
            "\"deaths\":3" +
            "}," +
            "{" +
            "\"name\":\"Player2\"," +
            "\"frags\":2," +
            "\"kills\":2," +
            "\"deaths\":21" +
            "}" +
            "]" +
            "}";

        public static readonly Match Match = new Match(
            "167.42.23.32-1337",
            DateTime.Parse("2017-01-22T15:11:12Z"),
            MatchResult);

        public const string SerialaizedMatch =
            "{" +
            "\"server\":\"167.42.23.32-1337\"," +
            "\"timestamp\":\"2017-01-22T15:11:12Z\"," +
            "\"results\":" +
            SerialaizedMatchResult +
            "}";

        public static readonly ServerStat ServerStat = new ServerStat(
            "167.42.23.32-1337",
            100500,
            33,
            (decimal)24.45624,
            32,
            (decimal)20.45,
            new[] {"DM", "TDM"},
            new[]
            {
                "DM-HelloWorld",
                "DM-1on1-Rose",
                "DM-Kitchen",
                "DM-Camper Paradise",
                "DM-Appalachian Wonderland"
            });

        public const string SerialaizedServerStat =
            "{" +
            "\"totalMatchesPlayed\":100500," +
            "\"maximumMatchesPerDay\":33," +
            "\"averageMatchesPerDay\":24.45624," +
            "\"maximumPopulation\":32," +
            "\"averagePopulation\":20.45," +
            "\"top5GameModes\":[\"DM\",\"TDM\"]," +
            "\"top5Maps\":[" +
            "\"DM-HelloWorld\"," +
            "\"DM-1on1-Rose\"," +
            "\"DM-Kitchen\"," +
            "\"DM-Camper Paradise\"," +
            "\"DM-Appalachian Wonderland\"" +
            "]" +
            "}";

        public static readonly PlayerStat PlayerStat = new PlayerStat(
            null,
            100500,
            1000,
            "62.210.26.88-1337",
            2,
            "DM",
            (decimal)76.145693,
            33,
            (decimal)24.45624,
            DateTime.Parse("2017-01-22T15:11:12Z"),
            (decimal)3.124333
        );

        public const string SerialaizedPlayerStat =
            "{" +
            "\"totalMatchesPlayed\":100500," +
            "\"totalMatchesWon\":1000," +
            "\"favoriteServer\":\"62.210.26.88-1337\"," +
            "\"uniqueServers\":2," +
            "\"favoriteGameMode\":\"DM\"," +
            "\"averageScoreboardPercent\":76.145693," +
            "\"maximumMatchesPerDay\":33," +
            "\"averageMatchesPerDay\":24.45624," +
            "\"lastMatchPlayed\":\"2017-01-22T15:11:12Z\"," +
            "\"killToDeathRatio\":3.124333" +
            "}";

        public static readonly BestPlayer BestPlayer = new BestPlayer("Player1", (decimal)3.124333);

        public const string SerialaizedBestPlayer =
            "{" +
            "\"name\":\"Player1\"," +
            "\"killToDeathRatio\":3.124333" +
            "}";

        public static readonly PopularServer PopularServer = new PopularServer(
            "62.210.26.88-1337",
            ">> Sniper Heaven <<",
            (decimal)24.456240);

        public const string SerialaizedPopularServer =
            "{" +
            "\"endpoint\":\"62.210.26.88-1337\"," +
            "\"name\":\">> Sniper Heaven <<\"," +
            "\"averageMatchesPerDay\":24.45624" +
            "}";
    }
}

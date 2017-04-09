using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Kontur.GameStats.Server.Domains;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Utils
{
    public static class Queries
    {
        public static string CreateTableServer => $@"
CREATE TABLE IF NOT EXISTS {Tables.Server} (
{Domains.Server.Properties.Endpoint} VARCHAR(100) PRIMARY KEY,
{ServerInfo.Properties.Name} VARCHAR(100),
{ServerInfo.Properties.GameModes} VARCHAR(100)
) WITHOUT ROWID";

        public static string CreateTableMatch => $@"
CREATE TABLE IF NOT EXISTS {Tables.Match} (
{Match.Properties.Server} VARCHAR(100),
{Match.Properties.Timestamp} DATETIME,
{MatchResult.Properties.Map} VARCHAR(100),
{MatchResult.Properties.GameMode} VARCHAR(6),
{MatchResult.Properties.FragLimit} INT,
{MatchResult.Properties.TimeLimit} INT,
{MatchResult.Properties.TimeElapsed} DECIMAL(10,6)
)";

        public static string CreateIndexMatchServer => $@"
CREATE INDEX IF NOT EXISTS {Tables.Match}_{Match.Properties.Server}
ON {Tables.Match}({Match.Properties.Server} ASC)";

        public static string CreateIndexMatchTimestamp => $@"
CREATE INDEX IF NOT EXISTS {Tables.Match}_{Match.Properties.Timestamp}
ON {Tables.Match}({Match.Properties.Timestamp} DESC)";

        public static string CreateTableScoreboard => $@"
CREATE TABLE IF NOT EXISTS {Tables.Scoreboard} (
{Scoreboard.Properties.Name} VARCHAR(100),
{Scoreboard.Properties.SearchName} VARCHAR(100),
{Scoreboard.Properties.Frags} INT,
{Scoreboard.Properties.Kills} INT,
{Scoreboard.Properties.Deaths} INT,
{Scoreboard.Properties.MatchId} INT,
{Scoreboard.Properties.Place} INT
)";

        public static string CreateIndexScoreboardSearchName => $@"
CREATE INDEX IF NOT EXISTS {Tables.Scoreboard}_{Scoreboard.Properties.SearchName}
ON {Tables.Scoreboard}({Scoreboard.Properties.SearchName} ASC)";

        public static string CreateIndexScoreboardMatchId => $@"
CREATE INDEX IF NOT EXISTS {Tables.Scoreboard}_{Scoreboard.Properties.MatchId}
ON  {Tables.Scoreboard}({Scoreboard.Properties.MatchId} ASC)";

        public static string CreateTablePlayer => $@"
CREATE TABLE IF NOT EXISTS {Tables.Player} (
{BestPlayer.Properties.Name} VARCHAR(100),
{BestPlayer.Properties.SearchName} VARCHAR(100) PRIMARY KEY,
{BestPlayer.Properties.Frags} INT,
{BestPlayer.Properties.Kills} INT,
{BestPlayer.Properties.Deaths} INT,
{BestPlayer.Properties.MatchCount} INT
) WITHOUT ROWID";

        public static string CreateIndexPlayerKillToDeathRatio => $@"
CREATE INDEX IF NOT EXISTS {Tables.Player}_{BestPlayer.Properties.KillToDeathRatio}
ON {Tables.Player}(1.0*{BestPlayer.Properties.Kills}/{BestPlayer.Properties.Deaths} DESC)";

        public static string DropTableServer => DropTableQuery(Tables.Server);

        public static string DropTableMatch => DropTableQuery(Tables.Match);

        public static string DropTableScoreboard => DropTableQuery(Tables.Scoreboard);

        public static string DropTablePlayer => DropTableQuery(Tables.Player);

        public static string InsertServer(Domains.Server server) => $@"
INSERT OR REPLACE INTO {Tables.Server} (
{Domains.Server.Properties.Endpoint},
{ServerInfo.Properties.Name},
{ServerInfo.Properties.GameModes}
) VALUES (
'{server.Endpoint}',
'{server.Info.Name}',
'{JsonConvert.SerializeObject(server.Info.GameModes)}'
);";

        public static string SelectServerInfo(string endpoint) => $@"
SELECT {ServerInfo.Properties.Name}, {ServerInfo.Properties.GameModes}
FROM {Tables.Server} 
WHERE {Domains.Server.Properties.Endpoint} = '{endpoint}'
LIMIT 1";

        public static string SelectAllServers => $@"
SELECT {Domains.Server.Properties.Endpoint}, {ServerInfo.Properties.Name}, {ServerInfo.Properties.GameModes}
FROM {Tables.Server}";

        public static string SelectMatchResult(Match match) => $@"
SELECT 
{MatchResult.Properties.Id},
{MatchResult.Properties.Map},
{MatchResult.Properties.GameMode},
{MatchResult.Properties.FragLimit},
{MatchResult.Properties.TimeLimit},
{MatchResult.Properties.TimeElapsed}
FROM {Tables.Match}
WHERE {Match.Properties.Server} = '{match.Server}'
AND {Match.Properties.Timestamp} = '{DateTimeForSqLite(match.Timestamp)}'
LIMIT 1";

        public static string SelectScoreboardForMatch(long matchId) => $@"
SELECT
{Scoreboard.Properties.Name},
{Scoreboard.Properties.Frags},
{Scoreboard.Properties.Kills},
{Scoreboard.Properties.Deaths} 
FROM {Tables.Scoreboard}
WHERE {Scoreboard.Properties.MatchId} = {matchId}
ORDER BY {Scoreboard.Properties.Place}";

        public static string GetMachesForServer(string endpoint) => $@"
SELECT
{Match.Properties.Timestamp},
COUNT({Tables.Match}.{MatchResult.Properties.Id}) AS {MatchResult.Properties.Population},
{MatchResult.Properties.GameMode},
{MatchResult.Properties.Map}
FROM {Tables.Match}
LEFT JOIN {Tables.Scoreboard} 
ON {Tables.Scoreboard}.{Scoreboard.Properties.MatchId} = {Tables.Match}.{MatchResult.Properties.Id}
WHERE {Match.Properties.Server} = '{endpoint}'
GROUP BY {Tables.Match}.{MatchResult.Properties.Id}";

        public static string SelectScoreboardForPlayerStat(string name) => $@"
SELECT
{scoreboard1}.{Scoreboard.Properties.Kills},
{scoreboard1}.{Scoreboard.Properties.Deaths},
{scoreboard1}.{Scoreboard.Properties.Place},
{Tables.Match}.{Match.Properties.Server},
{Tables.Match}.{Match.Properties.Timestamp},
{Tables.Match}.{MatchResult.Properties.GameMode},
COUNT({Tables.Match}.{MatchResult.Properties.Id}) AS {MatchResult.Properties.Population}
FROM {Tables.Scoreboard} as {scoreboard1}
LEFT JOIN {Tables.Match}
ON {scoreboard1}.{Scoreboard.Properties.MatchId} = {Tables.Match}.{MatchResult.Properties.Id}
LEFT JOIN {Tables.Scoreboard} as {scoreboard2}
ON {Tables.Match}.{MatchResult.Properties.Id} = {scoreboard2}.{Scoreboard.Properties.MatchId}
WHERE {scoreboard1}.{Scoreboard.Properties.SearchName} = '{name}'
GROUP BY {Tables.Match}.{MatchResult.Properties.Id}";

        public static string SelectPlayer(string name) => $@"
SELECT
{Tables.Player}.{BestPlayer.Properties.Name},
{Tables.Player}.{BestPlayer.Properties.MatchCount},
{Tables.Player}.{BestPlayer.Properties.Deaths},
{Tables.Player}.{BestPlayer.Properties.Frags},
{Tables.Player}.{BestPlayer.Properties.Kills}
FROM {Tables.Player}
WHERE {Tables.Player}.{BestPlayer.Properties.SearchName} = '{name}'
LIMIt 1";

        public static string LastMatchDate => $@"
SELECT {Match.Properties.Timestamp} FROM {Tables.Match}
ORDER BY {Match.Properties.Timestamp} DESC
LIMIT 1";

        public static string GetRecentMatches(int count) => $@"
SELECT
{MatchResult.Properties.Id},
{Match.Properties.Server},
{Match.Properties.Timestamp},
{MatchResult.Properties.Map},
{MatchResult.Properties.GameMode},
{MatchResult.Properties.FragLimit},
{MatchResult.Properties.TimeLimit},
{MatchResult.Properties.TimeElapsed}
FROM {Tables.Match}
ORDER BY {Tables.Match}.{Match.Properties.Timestamp} DESC
LIMIT {count}";

        public static string SelectPopularServers(int count, DateTime lastMatchDateTime) => $@"
SELECT 
{PopularServer.Properties.Endpoint},
{PopularServer.Properties.Name},
1.0*COUNT({Tables.Match}.{Match.Properties.Server})/
(julianday(date('{DateTimeForSqLite(lastMatchDateTime)}'))-
julianday(date(MIN({Tables.Match}.{Match.Properties.Timestamp}))) + 1)
AS {PopularServer.Properties.AverageMatchesPerDay}
FROM {Tables.Match}
LEFT JOIN {Tables.Server}
ON {Tables.Server}.{Domains.Server.Properties.Endpoint} = {Tables.Match}.{Match.Properties.Server}
GROUP BY {Tables.Match}.{Match.Properties.Server}
ORDER BY {PopularServer.Properties.AverageMatchesPerDay} DESC
LIMIT {count}";

        public static string SelectBestPlayers(int count) => $@"
SELECT {BestPlayer.Properties.Name},
1.0*{BestPlayer.Properties.Kills}/{BestPlayer.Properties.Deaths} AS {BestPlayer.Properties.KillToDeathRatio}
FROM {Tables.Player}
WHERE {BestPlayer.Properties.Deaths} > 0 AND {BestPlayer.Properties.MatchCount} >= 10
ORDER BY {BestPlayer.Properties.KillToDeathRatio} DESC
LIMIT {count}";

        public static string SelectExistMatch(Match match) => $@"
SELECT {MatchResult.Properties.Id} FROM {Tables.Match}
WHERE {Match.Properties.Server} = '{match.Server}'
AND {Match.Properties.Timestamp} = '{DateTimeForSqLite(match.Timestamp)}'
LIMIT 1";

        public static string SelectExistServer(string endpoint) => $@"
SELECT {Domains.Server.Properties.Endpoint} FROM {Tables.Server}
WHERE {Domains.Server.Properties.Endpoint} = '{endpoint}'
LIMIT 1";

        public static string InsertMatch(Match match) => $@"
INSERT INTO {Tables.Match} (
{Match.Properties.Server},
{Match.Properties.Timestamp},
{MatchResult.Properties.Map},
{MatchResult.Properties.GameMode},
{MatchResult.Properties.FragLimit},
{MatchResult.Properties.TimeLimit},
{MatchResult.Properties.TimeElapsed}
) VALUES (
'{match.Server}',
'{DateTimeForSqLite(match.Timestamp)}',
'{match.Results.Map}',
'{match.Results.GameMode}',
{match.Results.FragLimit},
{match.Results.TimeLimit},
{DecimalForSqLite(match.Results.TimeElapsed)}
)";

        public static string InsertScoreboard(List<Scoreboard> scoreboard, long matchId) => $@"
INSERT INTO {Tables.Scoreboard} (
{Scoreboard.Properties.Name},
{Scoreboard.Properties.SearchName},
{Scoreboard.Properties.Frags},
{Scoreboard.Properties.Kills},
{Scoreboard.Properties.Deaths},
{Scoreboard.Properties.MatchId},
{Scoreboard.Properties.Place}
) VALUES (
{string.Join("),(", scoreboard.Select((s, i) => $@"
'{s.Name}',
'{s.Name.ToLower()}',
{s.Frags},
{s.Kills},
{s.Deaths},
{matchId},
{i + 1}"))}
)";

        public static string InsertPlayers(List<BestPlayer> players) => $@"
INSERT OR REPLACE INTO {Tables.Player} (
{BestPlayer.Properties.Name},
{BestPlayer.Properties.SearchName},
{BestPlayer.Properties.Deaths},
{BestPlayer.Properties.Frags},
{BestPlayer.Properties.Kills},
{BestPlayer.Properties.MatchCount}
) VALUES (
{string.Join("),(", players.Select(player => $@"
'{player.Name}',
'{player.Name.ToLower()}',
{player.Deaths},
{player.Frags},
{player.Kills},
{player.MatchCount}"))}
);";

        public static string SelectLastInsertId => "SELECT last_insert_rowid()";

        private const string scoreboard1 = "s1";

        private const string scoreboard2 = "s2";

        private static class Tables
        {
            public const string Server = "server";

            public const string Match = "match";

            public const string Scoreboard = "scoreboard";

            public const string Player = "player";
        }
        private static string DropTableQuery(string name) => $@"DROP TABLE IF EXISTS {name}";

        private static string DateTimeForSqLite(DateTime dateTime)
            => $"{dateTime.ToUniversalTime():yyyy-MM-dd HH:mm:ss}";

        private static string DecimalForSqLite(decimal num) => num.ToString(CultureInfo.InvariantCulture);
    }
}

using System;
using System.Collections.Generic;
using System.Data.Common;
using Kontur.GameStats.Server.Domains;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Utils
{
    public static class DataReaderHelper
    {
        public static Domains.Server GetServer(this DbDataReader reader)
        {
            return new Domains.Server((string)reader[Domains.Server.Properties.Endpoint],
                GetServerInfo(reader));
        }

        public static ServerInfo GetServerInfo(this DbDataReader reader)
        {
            return new ServerInfo((string)reader[ServerInfo.Properties.Name],
                JsonConvert.DeserializeObject<string[]>((string)reader[ServerInfo.Properties.GameModes]));
        }

        public static MatchResult GetMatchResult(this DbDataReader reader)
        {
            return new MatchResult((string)reader[MatchResult.Properties.Map],
                (string)reader[MatchResult.Properties.GameMode],
                (int)reader[MatchResult.Properties.FragLimit],
                (int)reader[MatchResult.Properties.TimeLimit],
                (decimal)reader[MatchResult.Properties.TimeElapsed],
                new List<Scoreboard>(),
                (long)reader[MatchResult.Properties.Id]);
        }

        public static Scoreboard GetScoreboard(this DbDataReader reader)
        {
            return new Scoreboard((string)reader[Scoreboard.Properties.Name],
                (int)reader[Scoreboard.Properties.Frags],
                (int)reader[Scoreboard.Properties.Kills],
                (int)reader[Scoreboard.Properties.Deaths]);
        }

        public static Match GetMatchForServerStat(this DbDataReader reader)
        {
            return new Match((DateTime)reader[Match.Properties.Timestamp],
                new MatchResult((string)reader[MatchResult.Properties.Map],
                    (string)reader[MatchResult.Properties.GameMode],
                    (int)(long)reader[MatchResult.Properties.Population]));
        }

        public static Scoreboard GetScoreboardWithMatch(this DbDataReader reader)
        {
            return new Scoreboard((int)reader[Scoreboard.Properties.Kills],
                (int)reader[Scoreboard.Properties.Deaths],
                (int)reader[Scoreboard.Properties.Place],
                new Match((string)reader[Match.Properties.Server],
                    (DateTime)reader[Match.Properties.Timestamp],
                    new MatchResult((string)reader[MatchResult.Properties.GameMode],
                        (int)(long)reader[MatchResult.Properties.Population])));
        }

        public static Match GetMatch(this DbDataReader reader)
        {
            return new Match((string)reader[Match.Properties.Server],
                (DateTime)reader[Match.Properties.Timestamp],
                new MatchResult((string)reader[MatchResult.Properties.Map],
                    (string)reader[MatchResult.Properties.GameMode],
                    (int)reader[MatchResult.Properties.FragLimit],
                    (int)reader[MatchResult.Properties.TimeLimit],
                    (decimal)reader[MatchResult.Properties.TimeElapsed],
                    new List<Scoreboard>(),
                    (long)reader[MatchResult.Properties.Id]));
        }

        public static PopularServer GetPopularServer(this DbDataReader reader)
        {
            return new PopularServer(
                (string)reader[PopularServer.Properties.Endpoint],
                (string)reader[PopularServer.Properties.Name],
                (decimal)(double)reader[PopularServer.Properties.AverageMatchesPerDay]);
        }

        public static BestPlayer GetBestPlayer(this DbDataReader reader)
        {
            return new BestPlayer((string)reader[Scoreboard.Properties.Name],
                (decimal)(double)reader[BestPlayer.Properties.KillToDeathRatio]);
        }

        public static BestPlayer GetPlayer(this DbDataReader reader)
        {
            return new BestPlayer((string)reader[BestPlayer.Properties.Name],
                (int)reader[BestPlayer.Properties.Frags],
                (int)reader[BestPlayer.Properties.Kills],
                (int)reader[BestPlayer.Properties.Deaths],
                (int)reader[BestPlayer.Properties.MatchCount]);
        }
    }
}

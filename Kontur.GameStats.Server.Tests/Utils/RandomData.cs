using System;
using System.Collections.Generic;
using System.Linq;
using Kontur.GameStats.Server.Domains;

namespace Kontur.GameStats.Server.Tests.Utils
{
    public class RandomData
    {
        private readonly Random rnd = new Random();

        private readonly List<string> servers;
        private readonly List<string> gameModes;
        private readonly List<string> maps;
        private readonly List<string> players;
        public RandomData(int serverCount, int gameModeCount = 5, int mapCount = 5, int playerCount = 5)
        {
            servers = GetListByPattern("endpoint-", serverCount);
            gameModes = GetListByPattern("gm", gameModeCount);
            maps = GetListByPattern("map", mapCount);
            players = GetListByPattern("player", playerCount);
        }

        public List<Domains.Server> GetServers()
        {
            return
                servers.Select(
                           i =>
                               new Domains.Server(i,
                                   new ServerInfo(Guid.NewGuid().ToString(), GetUniqueRandomEnumerable(gameModes, gm => gm).ToArray())))
                       .ToList();
        }
        public Domains.Server GetRandomServer()
        {
            return new Domains.Server(GetRandomFromList(servers),
                new ServerInfo(Guid.NewGuid().ToString(), GetUniqueRandomEnumerable(gameModes, i => i).ToArray()));
        }

        public List<Match> GetUniqueRandomMatchesForServer(Domains.Server server, int count, int deltaDays = 1)
        {
            var secondsInDay = 60 * 60 * 24;
            var list = new List<Match>();
            while (list.Count != count)
            {
                var match = GetRandomMatchForServer(server,
                    DateTime.Parse($"{DateTime.UtcNow:s}").ToUniversalTime().AddSeconds(
                        rnd.Next(deltaDays * secondsInDay) - deltaDays * secondsInDay / 2));
                if (list.All(i => i.Timestamp != match.Timestamp)) list.Add(match);
            }

            return list;
        }

        public Match GetRandomMatchForServer(Domains.Server server, DateTime dateTime)
        {
            return
                new Match(server.Endpoint, dateTime.AddMilliseconds(-dateTime.Millisecond),
                    new MatchResult(GetRandomFromList(maps),
                        GetRandomFromList(gameModes),
                        rnd.Next(50),
                        rnd.Next(50),
                        Math.Round((decimal)rnd.NextDouble() * 15, 6),
                        GetUniqueRandomEnumerable(players, GetRandomScoreboard, 100).ToList()));
        }

        public Scoreboard GetRandomScoreboard(string player)
        {
            return new Scoreboard(player, rnd.Next(50), rnd.Next(50), rnd.Next(50));
        }

        private List<string> GetListByPattern(string pattern, int count)
        {
            return Enumerable.Range(1, count).Select(i => $"{pattern}{i}").ToList();
        }

        private string GetRandomFromList(List<string> list)
        {
            return list[rnd.Next(list.Count)];
        }

        private IEnumerable<T> GetUniqueRandomEnumerable<T>(List<string> list, Func<string, T> getItem,
            int maxLength = Int32.MaxValue)
        {
            var length = rnd.Next(1, Math.Min(list.Count, maxLength));
            var hashSet = new HashSet<string>();
            while (hashSet.Count < length) hashSet.Add(GetRandomFromList(list));
            return hashSet.Select(getItem);
        }
    }
}

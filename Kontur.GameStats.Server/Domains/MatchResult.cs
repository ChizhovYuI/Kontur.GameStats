using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class MatchResult
    {
        public MatchResult(
            string gameMode,
            int population)
        {
            GameMode = gameMode;
            Population = population;
        }

        public MatchResult(
            string map,
            string gameMode,
            int population)
        {
            Map = map;
            GameMode = gameMode;
            Population = population;
        }

        [JsonConstructor]
        public MatchResult(
            string map,
            string gameMode,
            int fragLimit,
            int timeLimit,
            decimal timeElapsed,
            List<Scoreboard> scoreboard,
            long? id = null)
        {
            Map = map;
            GameMode = gameMode;
            FragLimit = fragLimit;
            TimeLimit = timeLimit;
            TimeElapsed = timeElapsed;
            Scoreboard = scoreboard;
            Id = id;
        }

        [JsonIgnore]
        public long? Id { get; }

        [JsonIgnore]
        public int Population { get; set; }

        public string Map { get; }

        public string GameMode { get; }

        public int FragLimit { get; }

        public int TimeLimit { get; }

        public decimal TimeElapsed { get; }

        public List<Scoreboard> Scoreboard { get; set; }

        protected bool Equals(MatchResult other)
        {
            return string.Equals(Map, other.Map) &&
                   string.Equals(GameMode, other.GameMode) &&
                   FragLimit == other.FragLimit &&
                   TimeLimit == other.TimeLimit &&
                   TimeElapsed == other.TimeElapsed &&
                   (Scoreboard?.SequenceEqual(other.Scoreboard) ?? false);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((MatchResult)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Map?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (GameMode?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ FragLimit;
                hashCode = (hashCode * 397) ^ TimeLimit;
                hashCode = (hashCode * 397) ^ TimeElapsed.GetHashCode();
                hashCode = (hashCode * 397) ^ (Scoreboard?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string Id = "rowid";

            public const string Map = "map";

            public const string GameMode = "gameMode";

            public const string FragLimit = "fragLimit";

            public const string TimeLimit = "timeLimit";

            public const string TimeElapsed = "timeElapsed";

            public const string Population = "population";
        }
    }
}

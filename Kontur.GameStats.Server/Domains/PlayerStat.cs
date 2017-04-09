using System;
using Kontur.GameStats.Server.Inerfaces;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class PlayerStat : ICacheable
    {
        public PlayerStat() {}

        public PlayerStat(string name)
        {
            Name = name;
        }

        [JsonConstructor]
        public PlayerStat(
            string name,
            int totalMatchesPlayed,
            int totalMatchesWon,
            string favoriteServer,
            int uniqueServers,
            string favoriteGameMode,
            decimal averageScoreboardPercent,
            int maximumMatchesPerDay,
            decimal averageMatchesPerDay,
            DateTime lastMatchPlayed,
            decimal killToDeathRatio)
        {
            Name = name;
            TotalMatchesPlayed = totalMatchesPlayed;
            TotalMatchesWon = totalMatchesWon;
            FavoriteServer = favoriteServer;
            UniqueServers = uniqueServers;
            FavoriteGameMode = favoriteGameMode;
            AverageScoreboardPercent = averageScoreboardPercent;
            MaximumMatchesPerDay = maximumMatchesPerDay;
            AverageMatchesPerDay = averageMatchesPerDay;
            LastMatchPlayed = lastMatchPlayed;
            KillToDeathRatio = killToDeathRatio;
        }

        [JsonIgnore]
        public string Name { get; }

        [JsonIgnore]
        public string Key => Name?.ToLower();

        public int TotalMatchesPlayed { get; }

        public int TotalMatchesWon { get; }

        public string FavoriteServer { get; }

        public int UniqueServers { get; }

        public string FavoriteGameMode { get; }

        public decimal AverageScoreboardPercent { get; }

        public int MaximumMatchesPerDay { get; }

        public decimal AverageMatchesPerDay { get; }

        public DateTime LastMatchPlayed { get; }

        public decimal KillToDeathRatio { get; }

        protected bool Equals(PlayerStat other)
        {
            return TotalMatchesPlayed == other.TotalMatchesPlayed &&
                   TotalMatchesWon == other.TotalMatchesWon &&
                   string.Equals(FavoriteServer, other.FavoriteServer) &&
                   UniqueServers == other.UniqueServers &&
                   string.Equals(FavoriteGameMode, other.FavoriteGameMode) &&
                   AverageScoreboardPercent == other.AverageScoreboardPercent &&
                   MaximumMatchesPerDay == other.MaximumMatchesPerDay &&
                   AverageMatchesPerDay == other.AverageMatchesPerDay &&
                   LastMatchPlayed.Equals(other.LastMatchPlayed) &&
                   KillToDeathRatio == other.KillToDeathRatio;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((PlayerStat)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = TotalMatchesPlayed;
                hashCode = (hashCode * 397) ^ TotalMatchesWon;
                hashCode = (hashCode * 397) ^ (FavoriteServer?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ UniqueServers;
                hashCode = (hashCode * 397) ^ (FavoriteGameMode?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ AverageScoreboardPercent.GetHashCode();
                hashCode = (hashCode * 397) ^ MaximumMatchesPerDay;
                hashCode = (hashCode * 397) ^ AverageMatchesPerDay.GetHashCode();
                hashCode = (hashCode * 397) ^ LastMatchPlayed.GetHashCode();
                hashCode = (hashCode * 397) ^ KillToDeathRatio.GetHashCode();
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string TotalMatchesPlayed = "totalMatchesPlayed";

            public const string TotalMatchesWon = "totalMatchesWon";

            public const string FavoriteServer = "favoriteServer";

            public const string UniqueServers = "uniqueServers";

            public const string FavoriteGameMode = "favoriteGameMode";

            public const string AverageScoreboardPercent = "averageScoreboardPercent";

            public const string MaximumMatchesPerDay = "maximumMatchesPerDay";

            public const string AverageMatchesPerDay = "averageMatchesPerDay";

            public const string LastMatchPlayed = "lastMatchPlayed";

            public const string KillToDeathRatio = "killToDeathRatio";
        }
    }
}

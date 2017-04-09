using System.Linq;
using Kontur.GameStats.Server.Inerfaces;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class ServerStat : ICacheable
    {
        public ServerStat() { }

        public ServerStat(string endpoint)
        {
            Endpoint = endpoint;
        }

        [JsonConstructor]
        public ServerStat(
            string endpoint,
            int totalMatchesPlayed,
            int maximumMatchesPerDay,
            decimal averageMatchesPerDay,
            int maximumPopulation,
            decimal averagePopulation,
            string[] top5GameModes,
            string[] top5Maps)
        {
            Endpoint = endpoint;
            TotalMatchesPlayed = totalMatchesPlayed;
            MaximumMatchesPerDay = maximumMatchesPerDay;
            AverageMatchesPerDay = averageMatchesPerDay;
            MaximumPopulation = maximumPopulation;
            AveragePopulation = averagePopulation;
            Top5GameModes = top5GameModes;
            Top5Maps = top5Maps;
        }

        [JsonIgnore]
        public string Endpoint { get; }

        [JsonIgnore]
        public string Key => Endpoint;

        public int TotalMatchesPlayed { get; }

        public int MaximumMatchesPerDay { get; }

        public decimal AverageMatchesPerDay { get; }

        public int MaximumPopulation { get; }

        public decimal AveragePopulation { get; }

        public string[] Top5GameModes { get; } = { };

        public string[] Top5Maps { get; } = { };

        protected bool Equals(ServerStat other)
        {
            return TotalMatchesPlayed == other.TotalMatchesPlayed &&
                   MaximumMatchesPerDay == other.MaximumMatchesPerDay &&
                   AverageMatchesPerDay == other.AverageMatchesPerDay &&
                   MaximumPopulation == other.MaximumPopulation &&
                   AveragePopulation == other.AveragePopulation &&
                   (Top5GameModes?.SequenceEqual(other.Top5GameModes) ?? false) &&
                   (Top5Maps?.SequenceEqual(other.Top5Maps) ?? false);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((ServerStat)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = TotalMatchesPlayed;
                hashCode = (hashCode * 397) ^ MaximumMatchesPerDay;
                hashCode = (hashCode * 397) ^ AverageMatchesPerDay.GetHashCode();
                hashCode = (hashCode * 397) ^ MaximumPopulation;
                hashCode = (hashCode * 397) ^ AveragePopulation.GetHashCode();
                hashCode = (hashCode * 397) ^ (Top5GameModes?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Top5Maps?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string TotalMatchesPlayed = "totalMatchesPlayed";

            public const string MaximumMatchesPerDay = "maximumMatchesPerDay";

            public const string AverageMatchesPerDay = "averageMatchesPerDay";

            public const string MaximumPopulation = "maximumPopulation";

            public const string AveragePopulation = "averagePopulation";

            public const string Top5GameModes = "top5GameModes";

            public const string Top5Top5MapsGameModes = "top5Maps";
        }
    }
}

using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class BestPlayer
    {
        [JsonConstructor]
        public BestPlayer(string name, decimal killToDeathRatio)
        {
            Name = name;
            KillToDeathRatio = killToDeathRatio;
        }

        public BestPlayer(string name, int frags, int kills, int deaths, int matchCount)
        {
            Name = name;
            Frags = frags;
            Kills = kills;
            Deaths = deaths;
            MatchCount = matchCount;
        }

        public string Name { get; }

        public decimal KillToDeathRatio { get; }

        [JsonIgnore]
        public int Frags { get; }

        [JsonIgnore]
        public int Kills { get; }

        [JsonIgnore]
        public int Deaths { get; }

        [JsonIgnore]
        public int MatchCount { get; }

        protected bool Equals(BestPlayer other)
        {
            return string.Equals(Name, other.Name) && KillToDeathRatio == other.KillToDeathRatio;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((BestPlayer)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name?.GetHashCode() ?? 0) * 397) ^ KillToDeathRatio.GetHashCode();
            }
        }

        public static class Properties
        {
            public const string Name = "name";

            public const string KillToDeathRatio = "killToDeathRatio";

            public const string SearchName = "searchName";

            public const string Frags = "frags";

            public const string Kills = "kills";

            public const string Deaths = "deaths";

            public const string MatchCount = "matchCount";
        }
    }
}

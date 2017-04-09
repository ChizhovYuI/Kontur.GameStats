using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Domains
{
    public class Scoreboard
    {
        [JsonConstructor]
        public Scoreboard(string name, int frags, int kills, int deaths)
        {
            Name = name;
            Frags = frags;
            Kills = kills;
            Deaths = deaths;
        }

        public Scoreboard(int kills, int deaths, int place, Match match)
        {
            Kills = kills;
            Deaths = deaths;
            Match = match;
            Place = place;
        }

        public string Name { get; }

        public int Frags { get; }

        public int Kills { get; }

        public int Deaths { get; }

        [JsonIgnore]
        public Match Match { get; }

        [JsonIgnore]
        public int Place { get; }

        protected bool Equals(Scoreboard other)
        {
            return string.Equals(Name, other.Name) &&
                   Frags == other.Frags &&
                   Kills == other.Kills &&
                   Deaths == other.Deaths;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((Scoreboard)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Name?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ Frags;
                hashCode = (hashCode * 397) ^ Kills;
                hashCode = (hashCode * 397) ^ Deaths;
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string Name = "name";

            public const string SearchName = "searchName";

            public const string Frags = "frags";

            public const string Kills = "kills";

            public const string Deaths = "deaths";

            public const string MatchId = "matchId";

            public const string Place = "place";
        }
    }
}

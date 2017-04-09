
namespace Kontur.GameStats.Server.Domains
{
    public class PopularServer
    {
        public PopularServer(string endpoint, string name, decimal averageMatchesPerDay)
        {
            Endpoint = endpoint;
            Name = name;
            AverageMatchesPerDay = averageMatchesPerDay;
        }

        public string Endpoint { get; }

        public string Name { get; }

        public decimal AverageMatchesPerDay { get; }

        protected bool Equals(PopularServer other)
        {
            return string.Equals(Endpoint, other.Endpoint) &&
                   string.Equals(Name, other.Name) &&
                   AverageMatchesPerDay == other.AverageMatchesPerDay;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj.GetType() == GetType() && Equals((PopularServer)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Endpoint?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ AverageMatchesPerDay.GetHashCode();
                return hashCode;
            }
        }

        public static class Properties
        {
            public const string Endpoint = "endpoint";

            public const string Name = "name";

            public const string AverageMatchesPerDay = "averageMatchesPerDay";
        }
    }
}

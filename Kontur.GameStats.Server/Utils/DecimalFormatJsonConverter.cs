using System;
using Newtonsoft.Json;

namespace Kontur.GameStats.Server.Utils
{
    public class DecimalFormatJsonConverter : JsonConverter
    {
        private const int numberOfDecimals = 6;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var d = (decimal)value;
            var rounded = Math.Round(d, numberOfDecimals);
            writer.WriteValue(rounded);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            throw new NotImplementedException("Unnecessary because CanRead is false. The type will skip the converter.");
        }

        public override bool CanRead => false;

        public override bool CanConvert(Type objectType) => objectType == typeof(decimal);
    }
}

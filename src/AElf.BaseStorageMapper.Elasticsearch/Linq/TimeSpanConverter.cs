using Newtonsoft.Json;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq
{
    public class TimeSpanConverter : JsonConverter<TimeSpan>
    {
        public override void WriteJson(JsonWriter writer, TimeSpan value, JsonSerializer serializer)
        {
            writer.WriteValue(value.Ticks);
        }

        public override TimeSpan ReadJson(JsonReader reader, Type objectType, TimeSpan existingValue, bool hasExistingValue, JsonSerializer serializer)
        {
            if (reader.Value == null || reader.ValueType == null || reader.ValueType != typeof(long))
            {
                return default;
            }

            return new TimeSpan((long)reader.Value);
        }
    }
    
    public class TimeSpanNullableConverter : JsonConverter<TimeSpan?>
    {
        public override void WriteJson(JsonWriter writer, TimeSpan? value, JsonSerializer serializer)
        {
            writer.WriteValue(value?.Ticks);
        }

        public override TimeSpan? ReadJson(JsonReader reader, Type objectType, TimeSpan? existingValue, bool hasExistingValue, JsonSerializer serializer)
        {
            if (reader.Value == null || reader.ValueType == null || reader.ValueType != typeof(long))
            {
                return null;
            }

            return new TimeSpan((long)reader.Value);
        }
    }
}
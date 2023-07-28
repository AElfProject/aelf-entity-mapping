using Elasticsearch.Net;

namespace AElf.BaseStorageMapper.Options
{
    public class IndexSettingOptions
    {
        public int NumberOfShards { get; set; } = 1;

        public int NumberOfReplicas { get; set; } = 1;

        public Refresh Refresh { get; set; } = Refresh.False;

        public string IndexPrefix { get; set; }
    }
}
using Elasticsearch.Net;

namespace AElf.EntityMapping.Elasticsearch.Options;

public class ElasticsearchOptions
{
    public List<string> Uris { get; set; } = new List<string> { "http://127.0.0.1:9200" };
    public int NumberOfShards { get; set; } = 1;
    public int NumberOfReplicas { get; set; } = 1;
    public Refresh Refresh { get; set; } = Refresh.False;
    public int MaxResultWindow { get; set; } = 10000;
    public int TermsArrayMaxLength { get; set; } = 100;
}
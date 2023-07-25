using Elasticsearch.Net;
using Microsoft.Extensions.Options;
using Nest;

namespace AElf.BaseStorageMapper.Elasticsearch;

public interface IElasticsearchClientProvider
{
    IElasticClient GetClient();
}

public class ElasticsearchClientProvider : IElasticsearchClientProvider
{
    private readonly IElasticClient _elasticClient;

    public ElasticsearchClientProvider(IOptions<ElasticsearchOptions> options)
    {
        var uris = options.Value.Endpoints.ConvertAll(x => new Uri(x));
        var connectionPool = new StaticConnectionPool(uris);
        var settings = new ConnectionSettings(connectionPool);
        _elasticClient = new ElasticClient(settings);
    }

    public IElasticClient GetClient()
    {
        return _elasticClient;
    }
}
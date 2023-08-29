using AElf.EntityMapping.Elasticsearch.Options;
using Elasticsearch.Net;
using Microsoft.Extensions.Options;
using Nest;
using Volo.Abp.DependencyInjection;

namespace AElf.EntityMapping.Elasticsearch;

public interface IElasticsearchClientProvider
{
    IElasticClient GetClient();
}

public class ElasticsearchClientProvider : IElasticsearchClientProvider, ISingletonDependency
{
    private readonly IElasticClient _elasticClient;

    public ElasticsearchClientProvider(IOptions<ElasticsearchOptions> options)
    {
        var uris = options.Value.Uris.ConvertAll(x => new Uri(x));
        var connectionPool = new StaticConnectionPool(uris);
        var settings = new ConnectionSettings(connectionPool);
        _elasticClient = new ElasticClient(settings);
    }

    public IElasticClient GetClient()
    {
        return _elasticClient;
    }
}
using System.Text;
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
        var settings = new ConnectionSettings(connectionPool).DisableDirectStreaming();
            // .OnRequestCompleted(callDetails =>
            // {
            //     // Print Request DSL
            //     if (callDetails.RequestBodyInBytes != null)
            //     {
            //         Console.WriteLine($"Request JSON: {Encoding.UTF8.GetString(callDetails.RequestBodyInBytes)}");
            //     }
            //     // // Print Response Data
            //     // if (callDetails.ResponseBodyInBytes != null)
            //     // {
            //     //     Console.WriteLine($"Response JSON: {Encoding.UTF8.GetString(callDetails.ResponseBodyInBytes)}");
            //     // }
            // });
        _elasticClient = new ElasticClient(settings);
    }

    public IElasticClient GetClient()
    {
        return _elasticClient;
    }
}
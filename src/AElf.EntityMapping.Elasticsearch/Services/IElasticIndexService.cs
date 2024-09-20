namespace AElf.EntityMapping.Elasticsearch.Services;

public interface IElasticIndexService
{
    Task CreateIndexAsync(string indexName, Type indexEntityType, int shard = 1, int numberOfReplicas = 1,
        Dictionary<string, object> indexSettings = null);

    Task CreateIndexTemplateAsync(string indexTemplateName, string indexName, Type indexEntityType, int numberOfShards,
        int numberOfReplicas);

    Task DeleteIndexAsync(string collectionName = null, CancellationToken cancellationToken = default);
}
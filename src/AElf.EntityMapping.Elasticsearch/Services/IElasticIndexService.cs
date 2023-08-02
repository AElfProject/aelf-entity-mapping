namespace AElf.EntityMapping.Elasticsearch.Services;

public interface IElasticIndexService
{
    Task CreateIndexAsync(string indexName, Type indexEntityType, int shard = 1, int numberOfReplicas = 1);

    Task CreateIndexTemplateAsync(string indexTemplateName, string indexName, Type indexEntityType, int numberOfShards,
        int numberOfReplicas);

    Task CreateNonShardKeyRouteIndexAsync(Type indexEntityType, int numberOfShards,
        int numberOfReplicas);
    
    Task InitializeIndexMarkedFieldAsync(Type indexEntityType);
    
    string GetIndexMarkFieldCacheName(Type indexEntityType);

    string GetDefaultIndexName(Type indexEntityType);
    
    string GetNonShardKeyRouteIndexName(Type indexEntityType, string fieldName);
    
    bool IsShardingCollection(Type type);
}
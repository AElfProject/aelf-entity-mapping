namespace AElf.BaseStorageMapper.Elasticsearch.Services;

public interface IElasticIndexService
{
    Task CreateIndexAsync(string indexName, Type indexEntityType, int shard = 1, int numberOfReplicas = 1);

    Task CreateIndexTemplateAsync(string indexTemplateName, string indexName, Type indexEntityType, int numberOfShards,
        int numberOfReplicas);

    Task CreateNonShardKeyRouteIndexAsync(Type indexEntityType, int numberOfShards,
        int numberOfReplicas);
    
    Task InitializeIndexMarkedFieldAsync(Type indexEntityType);
    
    Task<string> GetIndexMarkFieldCacheNameAsync(Type indexEntityType);

    Task<string> GetDefaultIndexNameAsync(Type indexEntityType);
    
    Task<string> GetNonShardKeyRouteIndexNameAsync(Type indexEntityType, string fieldName);
}
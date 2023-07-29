namespace AElf.BaseStorageMapper.Elasticsearch.Services;

public interface IElasticIndexService
{
    Task CreateIndexAsync(string indexName, Type type, int shard = 1, int numberOfReplicas = 1);

    Task CreateIndexTemplateAsync(string indexTemplateName, Type type, int numberOfShards,
        int numberOfReplicas);
    
    Task InitializeIndexMarkedFieldAsync(Type type);
    
    Task<string> GetIndexMarkFieldNameAsync(Type type);
}
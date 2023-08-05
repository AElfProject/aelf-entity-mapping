using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public interface INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetShardCollectionNameListByConditionsAsync(List<CollectionNameCondition> conditions);

    Task<string> GetShardCollectionNameByIdAsync(string id);

    Task<List<CollectionMarkField>> GetNonShardKeysAsync();

    Task<NonShardKeyRouteCollection> GetNonShardKeyRouteIndexAsync(string id, string indexName, CancellationToken cancellationToken = default);
    
}
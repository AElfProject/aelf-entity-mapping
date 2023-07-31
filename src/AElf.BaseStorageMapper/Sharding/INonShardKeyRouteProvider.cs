namespace AElf.BaseStorageMapper.Sharding;

public interface INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetShardCollectionNameListByConditionsAsync(List<CollectionNameCondition> conditions);

    Task<string> GetShardCollectionNameListByIdAsync(string id);

    Task<List<CollectionMarkField>> GetNonShardKeysAsync();

    Task<NonShardKeyRouteIndex> GetNonShardKeyRouteIndexAsync(string id, string indexName);
}
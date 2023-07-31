namespace AElf.BaseStorageMapper.Sharding;

public interface INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetShardCollectionNameListByConditionsAsync<TEntity>(Dictionary<string,object> conditions);

    Task<List<string>> GetShardCollectionNameListByIdAsync<TEntity,TKey>(TKey id);

    Task<List<IndexMarkField>> GetNonShardKeysAsync();

    Task<NonShardKeyRouteIndex> GetNonShardKeyRouteIndexAsync(string id, string indexName);
}
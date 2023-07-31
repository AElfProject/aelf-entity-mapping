namespace AElf.BaseStorageMapper.Sharding;

public interface IShardingRouteProvider
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetShardCollectionListByEqualConditionAsync<TEntity>(Dictionary<string,object> conditions);
    
    Task<List<string>> GetShardCollectionListByGreaterThanConditionAsync<TEntity>(Dictionary<string,object> conditions);
    
    Task<List<string>> GetShardCollectionListByLessThanConditionAsync<TEntity>(Dictionary<string,object> conditions);
}
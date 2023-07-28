namespace AElf.BaseStorageMapper;

public interface IShardingRouteProvider
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetShardCollectionListByEqualConditionAsync<TEntity>(List<Dictionary<string,object>> conditions);
    
    Task<List<string>> GetShardCollectionListByGreaterThanConditionAsync<TEntity>(List<Dictionary<string,object>> conditions);
    
    Task<List<string>> GetShardCollectionListByLessThanConditionAsync<TEntity>(List<Dictionary<string,object>> conditions);
}
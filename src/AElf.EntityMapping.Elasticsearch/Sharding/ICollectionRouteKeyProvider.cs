using AElf.EntityMapping.Sharding;
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public interface ICollectionRouteKeyProvider<TEntity> where TEntity : class
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="conditions"></param>
    /// <typeparam name="TEntity">ElasticSearch Index Entity</typeparam>
    /// <returns></returns>
    Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions);

    Task<string> GetCollectionNameAsync(string id);

    Task<List<CollectionRouteKeyItem<TEntity>>> GetNonShardKeysAsync();

    Task<RouteKeyCollection> GetNonShardKeyRouteIndexAsync(string id, string indexName, CancellationToken cancellationToken = default);
    
    List<CollectionRouteKeyItem<TEntity>> NonShardKeys { get; set; }

    Task AddManyNonShardKeyRoute(List<TEntity> modelList, List<string> fullIndexNameList, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task AddNonShardKeyRoute(TEntity model, string fullIndexName, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task UpdateNonShardKeyRoute(TEntity model, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task DeleteManyNonShardKeyRoute(List<TEntity> modelList, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task DeleteNonShardKeyRoute(string id, IElasticClient client,
        CancellationToken cancellationToken = default);

}
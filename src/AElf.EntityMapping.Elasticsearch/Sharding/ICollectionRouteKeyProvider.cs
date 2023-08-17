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

    Task<List<CollectionRouteKeyItem<TEntity>>> GetCollectionRouteKeysAsync();

    Task<RouteKeyCollection> GetCollectionRouteKeyIndexAsync(string id, string indexName, CancellationToken cancellationToken = default);

    Task AddManyCollectionRouteKey(List<TEntity> modelList, List<string> fullIndexNameList, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task AddCollectionRouteKey(TEntity model, string fullIndexName, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task UpdateCollectionRouteKey(TEntity model, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task DeleteManyCollectionRouteKey(List<TEntity> modelList, IElasticClient client,
        CancellationToken cancellationToken = default);

    Task DeleteCollectionRouteKey(string id, IElasticClient client,
        CancellationToken cancellationToken = default);

}
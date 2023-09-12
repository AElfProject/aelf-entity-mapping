
using Nest;

namespace AElf.EntityMapping.Sharding;

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

    Task<List<CollectionRouteKeyItem<TEntity>>> GetCollectionRouteKeyItemsAsync();

    Task<IRouteKeyCollection> GetRouteKeyCollectionAsync(string id, string collectionName, CancellationToken cancellationToken = default);

    Task<List<BulkIndexOperation<IRouteKeyCollection>>> AddManyCollectionRouteKeyAsync(List<TEntity> modelList, List<string> fullCollectionNameList, CancellationToken cancellationToken = default);

    Task AddCollectionRouteKeyAsync(TEntity model, string fullCollectionName, CancellationToken cancellationToken = default);

    Task UpdateCollectionRouteKeyAsync(TEntity model, CancellationToken cancellationToken = default);

    Task<List<BulkDeleteOperation<IRouteKeyCollection>>> DeleteManyCollectionRouteKeyAsync(List<TEntity> modelList, CancellationToken cancellationToken = default);

    Task DeleteCollectionRouteKeyAsync(string id, CancellationToken cancellationToken = default);

}
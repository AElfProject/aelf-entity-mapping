
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

    Task<List<CollectionRouteKeyItem<TEntity>>> GetCollectionRouteKeysAsync();

    Task<IRouteKeyCollection> GetCollectionRouteKeyIndexAsync(string id, string collectionName, CancellationToken cancellationToken = default);

    Task AddManyCollectionRouteKey(List<TEntity> modelList, List<string> fullCollectionNameList, CancellationToken cancellationToken = default);

    Task AddCollectionRouteKey(TEntity model, string fullCollectionName, CancellationToken cancellationToken = default);

    Task UpdateCollectionRouteKey(TEntity model, CancellationToken cancellationToken = default);

    Task DeleteManyCollectionRouteKey(List<TEntity> modelList, CancellationToken cancellationToken = default);

    Task DeleteCollectionRouteKey(string id, CancellationToken cancellationToken = default);

}
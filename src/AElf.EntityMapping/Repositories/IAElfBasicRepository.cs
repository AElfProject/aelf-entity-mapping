using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Repositories
{
    public interface IAElfBasicRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        Task AddAsync(TEntity model, string collectionName = null, CancellationToken cancellationToken = default);
        
        Task AddOrUpdateAsync(TEntity model, string collectionName = null, CancellationToken cancellationToken = default);
        
        Task AddOrUpdateManyAsync(List<TEntity> list, string collectionName = null, CancellationToken cancellationToken = default);

        Task UpdateAsync(TEntity model, string collectionName = null, CancellationToken cancellationToken = default);

        Task DeleteAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default);
        
        Task DeleteAsync(TEntity model, string collectionName = null, CancellationToken cancellationToken = default);
        
        Task DeleteManyAsync(List<TEntity> list, string collectionName = null, CancellationToken cancellationToken = default);
    }
}
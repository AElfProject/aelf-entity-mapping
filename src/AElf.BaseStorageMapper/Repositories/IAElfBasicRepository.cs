using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Repositories
{
    public interface IAElfBasicRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        Task AddAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default);
        
        Task AddOrUpdateAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default);
        
        Task AddOrUpdateManyAsync(List<TEntity> list, string collection = null, CancellationToken cancellationToken = default);

        Task UpdateAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default);

        Task DeleteAsync(TKey id, string collection = null, CancellationToken cancellationToken = default);
        
        Task DeleteAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default);
        
        Task DeleteManyAsync(List<TEntity> list, string collection = null, CancellationToken cancellationToken = default);
    }
}
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Repositories
{
    public interface IAElfReadOnlyBasicRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        Task<TEntity> GetAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default);

        Task<List<TEntity>> GetListAsync(string collectionName = null, CancellationToken cancellationToken = default);

        Task<long> GetCountAsync(string collectionName = null, CancellationToken cancellationToken = default);
    }
}
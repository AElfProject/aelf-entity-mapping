using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Repositories
{
    public interface IAElfReadOnlyBasicRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        Task<TEntity> GetAsync(TKey id, string collection = null, CancellationToken cancellationToken = default);

        Task<List<TEntity>> GetListAsync(string collection = null, CancellationToken cancellationToken = default);

        Task<long> GetCountAsync(string collection = null, CancellationToken cancellationToken = default);
    }
}
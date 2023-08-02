using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Repositories
{
    public interface IAElfRepository<TEntity, TKey> : IAElfReadOnlyRepository<TEntity, TKey>,
        IAElfBasicRepository<TEntity, TKey>
        where TEntity : class, IEntity<TKey>
    {
    }
}
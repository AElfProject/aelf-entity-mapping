using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Repositories
{
    public interface IEntityMappingRepository<TEntity, TKey> : IEntityMappingReadOnlyRepository<TEntity, TKey>,
        IEntityMappingBasicRepository<TEntity, TKey>
        where TEntity : class, IEntity<TKey>
    {
    }
}
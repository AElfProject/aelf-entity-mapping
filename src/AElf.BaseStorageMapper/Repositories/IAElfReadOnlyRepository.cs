using System.Linq.Expressions;
using JetBrains.Annotations;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Repositories;

public interface IAElfReadOnlyRepository<TEntity, TKey> : IAElfReadOnlyBasicRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    Task<IQueryable<TEntity>> GetQueryableAsync(string collection = null, CancellationToken cancellationToken = default);
    
    Task<List<TEntity>> GetListAsync([NotNull]Expression<Func<TEntity, bool>> predicate, string collection = null,
        CancellationToken cancellationToken = default);

    Task<long> GetCountAsync([NotNull]Expression<Func<TEntity, bool>> predicate, string collection = null,
        CancellationToken cancellationToken = default);
}
using System.Linq.Expressions;
using JetBrains.Annotations;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Repositories;

public interface IAElfReadOnlyRepository<TEntity, TKey> : IAElfReadOnlyBasicRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    Task<IQueryable<TEntity>> GetQueryableAsync(string collectionName = null, CancellationToken cancellationToken = default);
    
    Task<List<TEntity>> GetListAsync([NotNull]Expression<Func<TEntity, bool>> predicate, string collectionName = null,
        CancellationToken cancellationToken = default);

    Task<long> GetCountAsync([NotNull]Expression<Func<TEntity, bool>> predicate, string collectionName = null,
        CancellationToken cancellationToken = default);
}
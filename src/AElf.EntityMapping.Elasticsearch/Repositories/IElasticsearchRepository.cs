using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Repositories;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public interface IElasticsearchRepository<TEntity, TKey> : IAElfRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default);
    
    Task<IElasticsearchQueryable<TEntity>> GetElasticsearchQueryableAsync(string collectionName = null, CancellationToken cancellationToken = default);
}
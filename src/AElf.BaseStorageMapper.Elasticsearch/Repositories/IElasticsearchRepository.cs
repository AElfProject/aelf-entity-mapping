using AElf.BaseStorageMapper.Elasticsearch.Linq;
using AElf.BaseStorageMapper.Repositories;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Repositories;

public interface IElasticsearchRepository<TEntity, TKey> : IAElfRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default);
    
    Task<IElasticsearchQueryable<TEntity, TKey>> GetElasticsearchQueryableAsync(string collectionName = null, CancellationToken cancellationToken = default);
}
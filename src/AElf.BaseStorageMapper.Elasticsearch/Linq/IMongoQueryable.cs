using System.Collections;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq;

public interface IElasticsearchQueryable<TEntity, TKey> : IQueryable<TEntity>
    where TEntity : class, IEntity<TKey>
{

}
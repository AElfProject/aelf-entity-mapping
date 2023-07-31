using AElf.BaseStorageMapper.Elasticsearch.Linq;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch;

public static class IElasticClientExtensions
{
    public static IElasticsearchQueryable<TEntity, TKey> AsQueryable<TEntity, TKey>(this IElasticClient elasticClient,
        ICollectionNameProvider<TEntity, TKey> collectionNameProvider, string index = null)
        where TEntity : class, IEntity<TKey>
    {
        return new ElasticsearchQueryable<TEntity, TKey>(elasticClient, collectionNameProvider, index);
    }
}
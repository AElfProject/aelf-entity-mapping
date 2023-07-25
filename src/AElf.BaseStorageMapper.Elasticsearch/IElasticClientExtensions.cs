using AElf.BaseStorageMapper.Elasticsearch.Linq;
using Nest;

namespace AElf.BaseStorageMapper.Elasticsearch;

public static class IElasticClientExtensions
{
    public static IElasticsearchQueryable<TEntity> AsQueryable<TEntity>(this IElasticClient elasticClient, string index)
        where TEntity : class
    {
        return new ElasticsearchQueryable<TEntity>(elasticClient, index);
    }
}
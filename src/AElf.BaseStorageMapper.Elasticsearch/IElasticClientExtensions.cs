using AElf.BaseStorageMapper.Elasticsearch.Linq;
using Nest;

namespace AElf.BaseStorageMapper.Elasticsearch;

public static class IElasticClientExtensions
{
    public static IElasticsearchQueryable<TEntity> AsQueryable<TEntity>(this IElasticClient elasticClient,
        IShardingRouteProvider shardingRouteProvider, string index = null)
        where TEntity : class
    {
        return new ElasticsearchQueryable<TEntity>(elasticClient, shardingRouteProvider, index);
    }
}
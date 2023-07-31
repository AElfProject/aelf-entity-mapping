using System.Linq.Expressions;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq
{
    public class ElasticsearchQueryable<T> : QueryableBase<T>, IElasticsearchQueryable<T>
    {
        public ElasticsearchQueryable(IElasticClient elasticClient, INonShardKeyRouteProvider shardingRouteProvider,
            string index)
            : base(new DefaultQueryProvider(typeof(ElasticsearchQueryable<>), QueryParser.CreateDefault(),
                new ElasticsearchQueryExecutor<T>(elasticClient, shardingRouteProvider, index)))
        {
        }

        public ElasticsearchQueryable(IQueryProvider provider, Expression expression)
            : base(provider, expression)
        {
        }
    }
}
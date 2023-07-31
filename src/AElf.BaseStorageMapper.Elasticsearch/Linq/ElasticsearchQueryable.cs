using System.Linq.Expressions;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq
{
    public class ElasticsearchQueryable<T> : QueryableBase<T>, IElasticsearchQueryable<T>
        where T : class, IEntity
    {
        public ElasticsearchQueryable(IElasticClient elasticClient, ICollectionNameProvider<T> collectionNameProvider,
            string index)
            : base(new DefaultQueryProvider(typeof(ElasticsearchQueryable<>), QueryParser.CreateDefault(),
                new ElasticsearchQueryExecutor<T>(elasticClient, collectionNameProvider, index)))
        {
        }

        public ElasticsearchQueryable(IQueryProvider provider, Expression expression)
            : base(provider, expression)
        {
        }
    }
}
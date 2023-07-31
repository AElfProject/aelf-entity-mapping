using System.Linq.Expressions;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq
{
    public class ElasticsearchQueryable<TEntity, TKey> : QueryableBase<TEntity>, IElasticsearchQueryable<TEntity, TKey>
        where TEntity : class, IEntity<TKey>
    {
        public ElasticsearchQueryable(IElasticClient elasticClient, ICollectionNameProvider<TEntity, TKey> collectionNameProvider,
            string index)
            : base(new DefaultQueryProvider(typeof(ElasticsearchQueryable<,>), QueryParser.CreateDefault(),
                new ElasticsearchQueryExecutor<TEntity, TKey>(elasticClient, collectionNameProvider, index)))
        {
        }

        public ElasticsearchQueryable(IQueryProvider provider, Expression expression)
            : base(provider, expression)
        {
        }
    }
}
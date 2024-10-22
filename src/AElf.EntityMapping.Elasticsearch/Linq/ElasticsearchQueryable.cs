using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Linq;
using Nest;
using Remotion.Linq;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class ElasticsearchQueryable<T> : QueryableBase<T>, IElasticsearchQueryable<T>
        where T : class, IEntity
    {
        public ElasticsearchQueryable(IElasticClient elasticClient, ICollectionNameProvider<T> collectionNameProvider,
            string index, ElasticsearchOptions elasticsearchOptions)
            : base(new DefaultQueryProvider(typeof(ElasticsearchQueryable<>),
                QueryParserFactory.Create(),
                new ElasticsearchQueryExecutor<T>(elasticClient, collectionNameProvider, index, elasticsearchOptions)))
        {
        }

        public ElasticsearchQueryable(IQueryProvider provider, Expression expression)
            : base(provider, expression)
        {
        }
    }
}
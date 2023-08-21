using System.Linq.Expressions;
using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingCollectionTailProvider<TEntity> : IShardingCollectionTailProvider<TEntity> where TEntity : class, IEntityMappingEntity
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    public ShardingCollectionTailProvider(IOptions<ElasticsearchOptions> indexSettingOptions,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingKeyProvider<TEntity>> logger)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
    }

    public ShardingCollectionTailProvider()
    {
    }
    private Task AddOrUpdateAsync(ShardingCollectionTail model)
    {
        var indexName = GetFullName(typeof(ShardingCollectionTail).Name);
        var client = _elasticsearchClientProvider.GetClient();
        var exits = client.DocumentExists(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

        if (exits.Exists)
        {
            var result = client.UpdateAsync(DocumentPath<ShardingCollectionTail>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_indexSettingOptions.Refresh));

            if (result.Result.IsValid) return Task.CompletedTask;
            throw new Exception($"Update Document failed at index{indexName} :" + result.Result.ServerError.Error.Reason);
        }
        else
        {
            var result = client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_indexSettingOptions.Refresh));
            if (result.Result.IsValid) return Task.CompletedTask;
            throw new Exception($"Insert Docuemnt failed at index {indexName} :" + result.Result.ServerError.Error.Reason);
        }
    }

    public async Task<Tuple<long, List<ShardingCollectionTail>>> GetShardingCollectionTailAsync(
        ShardingCollectionTail shardingCollectionTail)
    {
        var indexName = GetFullName(typeof(ShardingCollectionTail).Name);
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardingCollectionTail>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(shardingCollectionTail.EntityName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.TailPrefix).Value(shardingCollectionTail.TailPrefix)));
        QueryContainer Filter(QueryContainerDescriptor<ShardingCollectionTail> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest> selector = null;
        Expression<Func<ShardingCollectionTail, object>> sortExp = k => k.Tail;
        selector = new Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest>(s =>
            s.Index(indexName).Query(Filter).Sort(st => st.Field(sortExp, SortOrder.Descending)));

        var result = await client.SearchAsync(selector);
        _logger.LogInformation("ElasticsearchCollectionNameProvider.GetShardingCollectionTailAsync: searchDto: {shardingCollectionTail},indexName:{indexName},result:{result}", JsonConvert.SerializeObject(shardingCollectionTail),indexName, JsonConvert.SerializeObject(result));

        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }

        return new Tuple<long, List<ShardingCollectionTail>>(result.Total, result.Documents.ToList());
    }

    public async Task AddShardingCollectionTailAsync(string entityName, string keys, long tail)
    {
        var result = await GetShardingCollectionTailAsync(new ShardingCollectionTail(){EntityName = entityName, TailPrefix = keys});

        List<ShardingCollectionTail> shardingCollectionTailList = result.Item2;
        if (shardingCollectionTailList.IsNullOrEmpty())
        {
            var shardingCollectionTail = new ShardingCollectionTail();
            shardingCollectionTail.EntityName = entityName;
            shardingCollectionTail.TailPrefix = keys;
            shardingCollectionTail.Tail = tail;
            shardingCollectionTail.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(shardingCollectionTail);
            return;
        }

        var shardingCollection = shardingCollectionTailList.Find(a => a.TailPrefix.Contains(keys));

        if (shardingCollection != null && shardingCollection.Tail < tail)
        {
            shardingCollection.Tail = tail;
            await AddOrUpdateAsync(shardingCollection);
            return;
        }
    }
    private string GetFullName(string collectionName)
    {
        return (_aelfEntityMappingOptions.CollectionPrefix + ElasticsearchConstants.CollectionPrefixSplit + collectionName).ToLower();
    }
}
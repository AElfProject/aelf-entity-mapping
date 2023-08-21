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
using Volo.Abp.Caching;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingCollectionTailProvider<TEntity> : IShardingCollectionTailProvider<TEntity> where TEntity : class, IEntityMappingEntity
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;
    private readonly IDistributedCache<Dictionary<string,long>> _collectionTaildistributedCache;
    private readonly string _typeName = typeof(TEntity).Name.ToLower();

    public ShardingCollectionTailProvider(IOptions<ElasticsearchOptions> indexSettingOptions,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingKeyProvider<TEntity>> logger,IDistributedCache<Dictionary<string,long>> collectionTaildistributedCache)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionTaildistributedCache = collectionTaildistributedCache;
        _logger = logger;
    }

    public ShardingCollectionTailProvider()
    {
    }
    private async Task AddOrUpdateAsync(ShardingCollectionTail model)
    {
        var indexName = GetFullName(nameof(ShardingCollectionTail));
        var client = _elasticsearchClientProvider.GetClient();
        var exits = client.DocumentExists(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

        if (exits.Exists)
        {
            var result = await client.UpdateAsync(DocumentPath<ShardingCollectionTail>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_indexSettingOptions.Refresh));

            if (result.IsValid) return;
            throw new Exception($"Update Document failed at index{indexName} :" + result.ServerError.Error.Reason);
        }
        else
        {
            var result = client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_indexSettingOptions.Refresh));
            if (result.Result.IsValid) return;
            throw new Exception($"Insert Docuemnt failed at index {indexName} :" + result.Result.ServerError.Error.Reason);
        }
    }

    public async Task<long> GetShardingCollectionTailAsync(string tailPrefix)
    {
        tailPrefix = tailPrefix.ToLower();
        long tail = 0;
        var shardTailCache = _collectionTaildistributedCache.Get(_typeName);
        if (shardTailCache != null)
        {
            if (shardTailCache.TryGetValue(tailPrefix, out tail))
            {
                return tail;
            }
        }
        var result = await GetShardingCollectionTailByEsAsync(new ShardingCollectionTail(){EntityName = _typeName, TailPrefix = tailPrefix});
        if (result.Item1 > 0)
        {
            tail = result.Item2.First().Tail;
            if (shardTailCache == null)
            {
                shardTailCache = new Dictionary<string, long>();
            }

            shardTailCache.Add(tailPrefix, tail);
            _collectionTaildistributedCache.Set(_typeName, shardTailCache);
            return tail;
        }

        return tail;
    }

    private async Task<Tuple<long, List<ShardingCollectionTail>>> GetShardingCollectionTailByEsAsync(
        ShardingCollectionTail shardingCollectionTail)
    {
        var indexName = GetFullName(nameof(ShardingCollectionTail));
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardingCollectionTail>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(shardingCollectionTail.EntityName.ToLower())));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.TailPrefix).Value(shardingCollectionTail.TailPrefix.ToLower())));
        QueryContainer Filter(QueryContainerDescriptor<ShardingCollectionTail> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest> selector = null;
        Expression<Func<ShardingCollectionTail, object>> sortExp = k => k.Tail;
        //Only one item allowed
        selector = new Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest>(s =>
            s.Index(indexName).Query(Filter).Sort(st => st.Field(sortExp, SortOrder.Descending)).From(0).Size(1));

        var result = await client.SearchAsync(selector);
        _logger.LogInformation("ElasticsearchCollectionNameProvider.GetShardingCollectionTailAsync: searchDto: {shardingCollectionTail},indexName:{indexName},result:{result}", JsonConvert.SerializeObject(shardingCollectionTail),indexName, JsonConvert.SerializeObject(result));

        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }

        return new Tuple<long, List<ShardingCollectionTail>>(result.Total, result.Documents.ToList());
    }

    public async Task AddShardingCollectionTailAsync(string tailPrefix, long tail)
    {
        tailPrefix = tailPrefix.ToLower();
        var result = await GetShardingCollectionTailByEsAsync(new ShardingCollectionTail(){EntityName = _typeName, TailPrefix = tailPrefix});

        List<ShardingCollectionTail> shardingCollectionTailList = result.Item2;
        if (shardingCollectionTailList.IsNullOrEmpty())
        {
            var shardingCollectionTail = new ShardingCollectionTail();
            shardingCollectionTail.EntityName = _typeName;
            shardingCollectionTail.TailPrefix = tailPrefix.ToLower();
            shardingCollectionTail.Tail = tail;
            shardingCollectionTail.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(shardingCollectionTail);
            await ClearCacheAsync(_typeName);
            return;
        }

        var shardingCollection = shardingCollectionTailList.Find(a => a.TailPrefix.Contains(tailPrefix));

        if (shardingCollection != null && shardingCollection.Tail < tail)
        {
            shardingCollection.Tail = tail;
            await AddOrUpdateAsync(shardingCollection);
            await ClearCacheAsync(_typeName);
        }
    }
    private string GetFullName(string collectionName)
    {
        return (_aelfEntityMappingOptions.CollectionPrefix + ElasticsearchConstants.CollectionPrefixSplit + collectionName).ToLower();
    }

    private async Task ClearCacheAsync(string cacheKey)
    {
        await _collectionTaildistributedCache.RemoveAsync(cacheKey);
    }
}
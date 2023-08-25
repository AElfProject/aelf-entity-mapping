using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Caching.Distributed;
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
    private readonly ILogger<ShardingCollectionTailProvider<TEntity>> _logger;
    private readonly IDistributedCache<CollectionTailCacheItem> _collectionTailCache;
    private readonly Dictionary<string, long> _collectionTailCacheDictionary = new Dictionary<string, long>();
    private readonly string _typeName = typeof(TEntity).Name.ToLower();
    private const string CollectionTailCacheKeyPrefix = "CollectionTail";

    public ShardingCollectionTailProvider(IOptions<ElasticsearchOptions> indexSettingOptions,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingCollectionTailProvider<TEntity>> logger,IDistributedCache<CollectionTailCacheItem> collectionTailCache)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionTailCache = collectionTailCache;
        _logger = logger;
    }

    public ShardingCollectionTailProvider()
    {
    }
    private async Task AddOrUpdateAsync(ShardingCollectionTail model)
    {
        var indexName = GetFullName(nameof(ShardingCollectionTail));
        var client = _elasticsearchClientProvider.GetClient();
        var exits = await client.DocumentExistsAsync(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

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
        tailPrefix = tailPrefix.IsNullOrEmpty() ? _typeName : tailPrefix.ToLower();
        var cacheKey = GetCollectionTailCacheKey(tailPrefix);
        long tail = -1;
        var shardTailCacheItem = await GetCollectionTailCacheAsync(cacheKey);
        if (shardTailCacheItem != null)
        {
            tail = shardTailCacheItem.CollectionTail;
            _logger.LogDebug("ShardingCollectionTailProvider.GetShardingCollectionTailAsync--cache:cacheKey:{}, tailPrefix: {tailPrefix},tail:{tail}", cacheKey, tailPrefix, tail);
            return tail;
        }
        var result = await GetShardingCollectionTailByEsAsync(new ShardingCollectionTail(){EntityName = _typeName, TailPrefix = tailPrefix});
        if (!result.IsNullOrEmpty())
        {
            tail = result.First().Tail;
            _logger.LogInformation("ShardingCollectionTailProvider.GetShardingCollectionTailAsync--ES:cacheKey:{}, tailPrefix: {tailPrefix},tail:{tail}", cacheKey, tailPrefix, tail);
            await SetCollectionTailCacheAsync(tailPrefix, tail);
            return tail;
        }
        return tail;
    }

    private async Task<List<ShardingCollectionTail>> GetShardingCollectionTailByEsAsync(
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
        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }
        _logger.LogInformation("ElasticsearchCollectionNameProvider.GetShardingCollectionTailAsync: searchDto: {shardingCollectionTail},indexName:{indexName},result.Documents:{Documents}", JsonConvert.SerializeObject(shardingCollectionTail),indexName, JsonConvert.SerializeObject(result.Documents.ToList()));
        return result.Documents.ToList();
    }

    public async Task AddShardingCollectionTailAsync(string tailPrefix, long tail)
    {
        if (tail < 0)
        {
            return;
        }

        tailPrefix = tailPrefix.IsNullOrEmpty()?_typeName:tailPrefix.ToLower();
        var localCacheTail = GetAndUpdateLocalCacheTail(tailPrefix, tail);
        if(localCacheTail >= tail)
        {
            return;
        }
        
        var shardingCollectionTailList = await GetShardingCollectionTailByEsAsync(new ShardingCollectionTail(){EntityName = _typeName, TailPrefix = tailPrefix});
        if (shardingCollectionTailList.IsNullOrEmpty())
        {
            var shardingCollectionTail = new ShardingCollectionTail();
            shardingCollectionTail.EntityName = _typeName;
            shardingCollectionTail.TailPrefix = tailPrefix;
            shardingCollectionTail.Tail = tail;
            shardingCollectionTail.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(shardingCollectionTail);
            await SetCollectionTailCacheAsync(tailPrefix,tail);
            _logger.LogInformation("ElasticsearchCollectionNameProvider.AddShardingCollectionTailAsync--ADD: tailPrefix: {tailPrefix},tail:{tail},shardingCollectionTailList:{shardingCollectionTailList}", tailPrefix,tail, JsonConvert.SerializeObject(shardingCollectionTailList));
            return;
        }

        var shardingCollection = shardingCollectionTailList.Find(a => a.TailPrefix == tailPrefix);

        if (shardingCollection != null && shardingCollection.Tail < tail)
        {
            shardingCollection.Tail = tail;
            await AddOrUpdateAsync(shardingCollection);
            await SetCollectionTailCacheAsync(tailPrefix,tail);
            _logger.LogInformation("ElasticsearchCollectionNameProvider.AddShardingCollectionTailAsync--Update: tailPrefix: {tailPrefix},tail:{tail},shardingCollectionTailList:{shardingCollectionTailList}", tailPrefix,tail, JsonConvert.SerializeObject(shardingCollectionTailList));
        }
    }
    private string GetFullName(string collectionName)
    {
        return (_aelfEntityMappingOptions.CollectionPrefix + ElasticsearchConstants.CollectionPrefixSplit + collectionName).ToLower();
    }

    private string GetCollectionTailCacheKey(string tailPrefix)
    {
        var cacheKey = $"{CollectionTailCacheKeyPrefix}_{_typeName}_{tailPrefix}";
        return cacheKey.ToLower();
    }

    private async Task SetCollectionTailCacheAsync(string tailPrefix, long tail)
    {
        var cacheKey = GetCollectionTailCacheKey(tailPrefix);
        var shardTailCacheItem = await GetCollectionTailCacheAsync(cacheKey);
        
        if (shardTailCacheItem == null || tail > shardTailCacheItem.CollectionTail)
        {
            shardTailCacheItem = new CollectionTailCacheItem()
            {
                CollectionTail = tail
            };

            DistributedCacheEntryOptions cacheOptions = null;
            if (_aelfEntityMappingOptions.CollectionTailSecondExpireTime > 0)
            {
                var expireTime = DateTimeOffset.Now.AddSeconds(1000);
                cacheOptions = new DistributedCacheEntryOptions().SetAbsoluteExpiration(expireTime);
            }

            await _collectionTailCache.SetAsync(cacheKey, shardTailCacheItem, cacheOptions);
            _logger.LogInformation("ShardingCollectionTailProvider.SetCollectionTailCacheAsync:cacheKey:{cacheKey}, tailPrefix: {tailPrefix},tail:{tail}", cacheKey,tailPrefix,tail);
        }
    }

    private async Task<CollectionTailCacheItem> GetCollectionTailCacheAsync(string cacheKey)
    {
        return await _collectionTailCache.GetAsync(cacheKey);
    }

    private long GetAndUpdateLocalCacheTail(string tailPrefix, long targetTail)
    {
        long localTail = -1;
        if (_collectionTailCacheDictionary.TryGetValue(tailPrefix, out localTail))
        {
            if(targetTail > localTail)
            {
                _collectionTailCacheDictionary[tailPrefix] = targetTail;
            }
        }
        else
        {
            _collectionTailCacheDictionary.Add(tailPrefix, targetTail);
            localTail = -1;
        }

        return localTail;
    }
}
using System.Net.NetworkInformation;
using AElf.BaseStorageMapper.Elasticsearch.Repositories;
using AElf.BaseStorageMapper.Elasticsearch.Services;
using AElf.BaseStorageMapper.Sharding;
using Volo.Abp.Caching;

namespace AElf.BaseStorageMapper.Elasticsearch.Sharding;

public class NonShardKeyRouteProvider<TEntity> where TEntity : class
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<IndexMarkField>> _indexMarkFieldCache;
    private readonly IElasticsearchRepository<NonShardKeyRouteIndex,string> _nonShardKeyRouteIndexRepository;

    public NonShardKeyRouteProvider(IDistributedCache<List<IndexMarkField>> indexMarkFieldCache,
        IElasticIndexService elasticIndexService,
        IElasticsearchRepository<NonShardKeyRouteIndex, string> nonShardKeyRouteIndexRepository)
    {
        _indexMarkFieldCache = indexMarkFieldCache;
        _elasticIndexService = elasticIndexService;
        _nonShardKeyRouteIndexRepository = nonShardKeyRouteIndexRepository;
    }

    public async Task<List<IndexMarkField>> GetNonShardKeysAsync()
    {
        var indexMarkFieldsCacheKey = _elasticIndexService.GetIndexMarkFieldCacheName(typeof(TEntity));
        var indexMarkFields = await _indexMarkFieldCache.GetAsync(indexMarkFieldsCacheKey);
        if (indexMarkFields == null)
        {
            throw new Exception($"{typeof(TEntity).Name} Index marked field cache not found.");
        }

        return indexMarkFields;
    }
    
    public async Task<NonShardKeyRouteIndex> GetNonShardKeyRouteIndexAsync(string id,string indexName)
    {
        return await _nonShardKeyRouteIndexRepository.GetAsync(id, indexName);
    }
}
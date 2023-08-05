using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Sharding;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private readonly INonShardKeyRouteProvider<TEntity> _nonShardKeyRouteProvider;

    public ElasticsearchCollectionNameProvider(IElasticIndexService elasticIndexService,
        IShardingKeyProvider<TEntity> shardingKeyProvider, INonShardKeyRouteProvider<TEntity> nonShardKeyRouteProvider)
    {
        _elasticIndexService = elasticIndexService;
        _shardingKeyProvider = shardingKeyProvider;
        _nonShardKeyRouteProvider = nonShardKeyRouteProvider;
    }

    private string GetDefaultCollectionName()
    {
        return _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
    }
    
    protected override List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        if(conditions==null || conditions.Count==0)
            return new List<string>{GetDefaultCollectionName()};

        if (_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            var shardKeyCollectionNames= new List<string>();
            var nonShardKeyCollectionNames= new List<string>();

            AsyncHelper.RunSync(async () =>
            {
                shardKeyCollectionNames = _shardingKeyProvider.GetCollectionName(conditions);
                nonShardKeyCollectionNames =
                    await _nonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync(conditions);
            });

            if (shardKeyCollectionNames.Count > 0 && nonShardKeyCollectionNames.Count > 0)
            {
                return shardKeyCollectionNames.Intersect(nonShardKeyCollectionNames).ToList();
            }

            return shardKeyCollectionNames.Concat(nonShardKeyCollectionNames).ToList();
        }

        return new List<string>{GetDefaultCollectionName()};

    }

    protected override List<string> GetCollectionNameByEntity(TEntity entity)
    {
        if(entity==null)
            return new List<string>{GetDefaultCollectionName()};
        
        if (_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            var shardKeyCollectionName = "";
            var nonShardKeyCollectionNames= new List<string>();

            AsyncHelper.RunSync(async () =>
            {
                shardKeyCollectionName = _shardingKeyProvider.GetCollectionName(entity);
            });
            
            return new List<string>(){ shardKeyCollectionName };
        }
        
        return new List<string>{GetDefaultCollectionName()};
    }
    
    protected override List<string> GetCollectionNameByEntity(List<TEntity> entitys)
    {
        if(entitys == null || entitys.Count==0)
            return new List<string>{GetDefaultCollectionName()};
        
        if (_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            var shardKeyCollectionName = new List<string>();
            var nonShardKeyCollectionNames= new List<string>();

            AsyncHelper.RunSync(async () =>
            {
                shardKeyCollectionName = _shardingKeyProvider.GetCollectionName(entitys);
            });
            
            return shardKeyCollectionName;
        }
        
        return new List<string>{GetDefaultCollectionName()};
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        if (_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            string collectionName = string.Empty;
            AsyncHelper.RunSync(async () =>
            {
                collectionName=await _nonShardKeyRouteProvider.GetShardCollectionNameByIdAsync(id.ToString());
            });
            return collectionName;
        }
        
        return GetDefaultCollectionName();
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}
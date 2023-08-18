using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.Elasticsearch.Services;

public class EnsureIndexBuildService: IEnsureIndexBuildService, ITransientDependency
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly List<Type> _modules;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly AElfEntityMappingOptions _entityMappingOptions;
    
    
    public EnsureIndexBuildService(IOptions<CollectionCreateOptions> moduleConfiguration,
        IElasticIndexService elasticIndexService,
        IOptions<AElfEntityMappingOptions> entityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions)
    {
        _elasticIndexService = elasticIndexService;
        _modules = moduleConfiguration.Value.Modules;
        _elasticsearchOptions = elasticsearchOptions.Value;
        _entityMappingOptions = entityMappingOptions.Value;
    }
    
    public void EnsureIndexesCreate()
    {
        AsyncHelper.RunSync(async () =>
        {
            foreach (var module in _modules)
            {
                await HandleModuleAsync(module);
            }
        });
    }
    
    private async Task HandleModuleAsync(Type moduleType)
    {
        var types = GetTypesAssignableFrom<IEntityMappingEntity>(moduleType.Assembly);
        foreach (var t in types)
        {
            var indexName = IndexNameHelper.GetDefaultFullIndexName(t,_entityMappingOptions.CollectionPrefix);
            
            if (IsShardingCollection(t))
            {
                //if shard index, create index Template
                var indexTemplateName = indexName + "-template";
                await _elasticIndexService.CreateIndexTemplateAsync(indexTemplateName,indexName, t,
                    _elasticsearchOptions.NumberOfShards,
                    _elasticsearchOptions.NumberOfReplicas);
                //create index marked field cache
                // await _elasticIndexService.InitializeCollectionRouteKeyCacheAsync(t);
                //create non shard key route index
                await _elasticIndexService.CreateCollectionRouteKeyIndexAsync(t, _elasticsearchOptions.NumberOfShards,
                    _elasticsearchOptions.NumberOfReplicas);
                await CreateShardingCollectionTailIndexAsync();
            }
            else
            {
                await _elasticIndexService.CreateIndexAsync(indexName, t, _elasticsearchOptions.NumberOfShards,
                    _elasticsearchOptions.NumberOfReplicas);
            }
            
        }
        
    }

    private async Task CreateShardingCollectionTailIndexAsync()
    {
        var indexName = (_entityMappingOptions.CollectionPrefix + "." + typeof(ShardingCollectionTail).Name).ToLower();
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(ShardingCollectionTail),
            _elasticsearchOptions.NumberOfShards, _elasticsearchOptions.NumberOfReplicas);
    }

    private List<Type> GetTypesAssignableFrom<T>(Assembly assembly)
    {
        var compareType = typeof(T);
        return assembly.DefinedTypes
            .Where(type => compareType.IsAssignableFrom(type) && !compareType.IsAssignableFrom(type.BaseType) &&
                           !type.IsAbstract && type.IsClass && compareType != type)
            .Cast<Type>().ToList();
    }
    
    private bool IsShardingCollection(Type type)
    {
        if (_entityMappingOptions == null || _entityMappingOptions.ShardInitSettings == null)
            return false;
        var options = _entityMappingOptions.ShardInitSettings.Find(a => a.CollectionName == type.Name);
        return options != null;
    }
    

}
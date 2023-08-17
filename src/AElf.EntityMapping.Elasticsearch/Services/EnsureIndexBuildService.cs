using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Options;
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
            
            if (_entityMappingOptions.IsShardingCollection(t))
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
            }
            else
            {
                await _elasticIndexService.CreateIndexAsync(indexName, t, _elasticsearchOptions.NumberOfShards,
                    _elasticsearchOptions.NumberOfReplicas);
            }
            
        }
    }

    private List<Type> GetTypesAssignableFrom<T>(Assembly assembly)
    {
        var compareType = typeof(T);
        return assembly.DefinedTypes
            .Where(type => compareType.IsAssignableFrom(type) && !compareType.IsAssignableFrom(type.BaseType) &&
                           !type.IsAbstract && type.IsClass && compareType != type)
            .Cast<Type>().ToList();
    }
    

}
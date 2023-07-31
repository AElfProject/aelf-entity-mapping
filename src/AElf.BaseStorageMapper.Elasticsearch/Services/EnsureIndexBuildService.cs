using System.Reflection;
using AElf.BaseStorageMapper.Options;
using Microsoft.Extensions.Options;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Threading;

namespace AElf.BaseStorageMapper.Elasticsearch.Services;

public class EnsureIndexBuildService: IEnsureIndexBuildService, ITransientDependency
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly List<Type> _modules;
    private readonly IndexSettingOptions _indexSettingOptions;
    
    
    public EnsureIndexBuildService(IOptions<IndexCreateOption> moduleConfiguration,
        IElasticIndexService elasticIndexService, IOptions<IndexSettingOptions> indexSettingOptions)
    {
        _elasticIndexService = elasticIndexService;
        _modules = moduleConfiguration.Value.Modules;
        _indexSettingOptions = indexSettingOptions.Value;
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
        var types = GetTypesAssignableFrom<IIndexBuild>(moduleType.Assembly);
        foreach (var t in types)
        {
            var indexName = await _elasticIndexService.GetDefaultIndexNameAsync(t);
            await _elasticIndexService.CreateIndexAsync(indexName, t, _indexSettingOptions.NumberOfShards,
                _indexSettingOptions.NumberOfReplicas);

            //TODO: if shard index, create index Template
            var indexTemplateName = "." + indexName + "-template";
            await _elasticIndexService.CreateIndexTemplateAsync(indexTemplateName,indexName, t,
                _indexSettingOptions.NumberOfShards,
                _indexSettingOptions.NumberOfReplicas);
            //create index marked field cache
            await _elasticIndexService.InitializeIndexMarkedFieldAsync(t);
            //create non shard key route index
            await _elasticIndexService.CreateNonShardKeyRouteIndexAsync(t, _indexSettingOptions.NumberOfShards,
                _indexSettingOptions.NumberOfReplicas);
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
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
            var indexName = _indexSettingOptions.IndexPrefix.IsNullOrWhiteSpace()
                ? t.Name.ToLower()
                : $"{_indexSettingOptions.IndexPrefix.ToLower()}.{t.Name.ToLower()}";
            await _elasticIndexService.CreateIndexAsync(indexName, t, _indexSettingOptions.NumberOfShards,
                _indexSettingOptions.NumberOfReplicas);

            //TODO: if shard index, create index Template
            var indexTemplateName = indexName;
            await _elasticIndexService.CreateIndexTemplateAsync(indexTemplateName, t,
                _indexSettingOptions.NumberOfShards,
                _indexSettingOptions.NumberOfReplicas);
            await _elasticIndexService.InitializeIndexMarkedFieldAsync(t);
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
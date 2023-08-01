using AElf.BaseStorageMapper.Elasticsearch.Options;
using AElf.BaseStorageMapper.Elasticsearch.Repositories;
using AElf.BaseStorageMapper.Elasticsearch.Services;
using AElf.BaseStorageMapper.Elasticsearch.Sharding;
using AElf.BaseStorageMapper.Repositories;
using AElf.BaseStorageMapper.Sharding;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Caching;
using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper.Elasticsearch;

[DependsOn(
    typeof(AElfBaseStorageMapperModule),
    typeof(AbpCachingModule)
)]
public class AElfBaseStorageMapperElasticsearchModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        var services = context.Services;
        services.AddTransient(typeof(IAElfRepository<,>), typeof(ElasticsearchRepository<,>));
        services.AddTransient(typeof(IElasticsearchRepository<,>), typeof(ElasticsearchRepository<,>));
        services.AddTransient(typeof(ICollectionNameProvider<>), typeof(ElasticsearchCollectionNameProvider<>));
        services.AddTransient(typeof (IShardingKeyProvider<>), typeof (ShardingKeyProvider<>));
        services.AddTransient(typeof(INonShardKeyRouteProvider<>), typeof(NonShardKeyRouteProvider<>));
        services.AddTransient(typeof(IElasticIndexService), typeof(ElasticIndexService));
        services.AddTransient(typeof(IEnsureIndexBuildService), typeof(EnsureIndexBuildService));
        var configuration = context.Services.GetConfiguration();
        Configure<ElasticsearchOptions>(configuration.GetSection("Elasticsearch"));
    }

    public override void OnPreApplicationInitialization(ApplicationInitializationContext context)
    {
        var ensureIndexBuildService = context.ServiceProvider.GetService<IEnsureIndexBuildService>();
        ensureIndexBuildService?.EnsureIndexesCreate();
    }
}
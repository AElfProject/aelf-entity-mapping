using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Repositories;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Caching;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping.Elasticsearch;

[DependsOn(
    typeof(AElfEntityMappingModule),
    typeof(AbpCachingModule)
)]
public class AElfEntityMappingElasticsearchModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        var services = context.Services;
        services.AddTransient(typeof(IEntityMappingRepository<,>), typeof(ElasticsearchRepository<,>));
        services.AddTransient(typeof(IElasticsearchRepository<,>), typeof(ElasticsearchRepository<,>));
        services.AddTransient(typeof(ICollectionNameProvider<>), typeof(ElasticsearchCollectionNameProvider<>));
        services.AddSingleton(typeof (IShardingKeyProvider<>), typeof (ShardingKeyProvider<>));
        services.AddSingleton(typeof(ICollectionRouteKeyProvider<>), typeof(CollectionKeyProvider<>));
        services.AddSingleton(typeof(IElasticsearchQueryableFactory<>), typeof(ElasticsearchQueryableFactory<>));
        var configuration = context.Services.GetConfiguration();
        Configure<ElasticsearchOptions>(configuration.GetSection("Elasticsearch"));
    }

    public override void OnPreApplicationInitialization(ApplicationInitializationContext context)
    {
        var ensureIndexBuildService = context.ServiceProvider.GetService<IEnsureIndexBuildService>();
        ensureIndexBuildService?.EnsureIndexesCreate();
    }
}
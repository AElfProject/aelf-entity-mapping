using AElf.BaseStorageMapper.Elasticsearch.Options;
using AElf.BaseStorageMapper.Options;
using AElf.BaseStorageMapper.TestBase;
using Elasticsearch.Net;
using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper.Elasticsearch;

[DependsOn(typeof(AElfBaseStorageMapperElasticsearchModule),
    typeof(AElfBaseStorageMapperTestBaseModule)
)]
public class AElfElasticsearchTestsModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        
        Configure<CollectionCreateOption>(x =>
        {
            x.AddModule(typeof(AElfElasticsearchTestsModule));
        });
        
        Configure<ElasticsearchOptions>(options =>
        {
            options.NumberOfReplicas = 1;
            options.NumberOfShards = 5;
            options.Refresh = Refresh.True;
            // options.IndexPrefix = "AElfIndexer";
        });
        
        Configure<AElfBaseStorageMapperOptions>(options =>
        {
            options.CollectionPrefix = "AElfIndexer";
        });
    }
}
using AElf.BaseStorageMapper.TestBase;
using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper.Elasticsearch;

[DependsOn(typeof(AElfBaseStorageMapperElasticsearchModule),
    typeof(AElfBaseStorageMapperTestBaseModule)
)]
public class AElfElasticsearchTestsModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
    }
}
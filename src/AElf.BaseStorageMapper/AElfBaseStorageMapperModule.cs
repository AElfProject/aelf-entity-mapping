using AElf.BaseStorageMapper.Options;
using AElf.BaseStorageMapper.Sharding;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Caching;
using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper
{
    
    [DependsOn(
        typeof(AbpCachingModule)
    )]
    public class AElfBaseStorageMapperModule : AbpModule
    {
        public override void ConfigureServices(ServiceConfigurationContext context)
        {
            var configuration = context.Services.GetConfiguration();
            Configure<AElfBaseStorageMapperOptions>(configuration.GetSection("BaseStorageMapper"));
            
            var services = context.Services;
            services.AddTransient(typeof(ICollectionNameProvider<>), typeof(DefaultCollectionNameProvider<>));
            
            // Configure<IndexSettingOptions>(configuration.GetSection("IndexSetting"));
            Configure<ShardInitSettingOptions>(configuration.GetSection("ShardSetting"));
        }
        
        public override void OnPreApplicationInitialization(ApplicationInitializationContext context)
        {
            
        }
        
    }
}
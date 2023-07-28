using AElf.BaseStorageMapper.Options;
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Authorization;
using Volo.Abp.Autofac;
using Volo.Abp.BackgroundJobs;
using Volo.Abp.Data;
using Volo.Abp.Modularity;
using Volo.Abp.Threading;

namespace AElf.BaseStorageMapper.TestBase
{
    [DependsOn(
        typeof(AbpAutofacModule),
        typeof(AbpTestBaseModule),
        typeof(AElfBaseStorageMapperModule),
        typeof(AbpAuthorizationModule))]
    public class AElfBaseStorageMapperTestBaseModule : AbpModule
    {
        public override void PreConfigureServices(ServiceConfigurationContext context)
        {
        }

        public override void ConfigureServices(ServiceConfigurationContext context)
        {
            Configure<AbpBackgroundJobOptions>(options =>
            {
                options.IsJobExecutionEnabled = false;
            });

            context.Services.AddAlwaysAllowAuthorization();
            context.Services.Configure<EsEndpointOption>(options =>
            {
                options.Uris = new List<string> { "http://127.0.0.1:9200" };
            });

            context.Services.Configure<IndexSettingOptions>(options =>
            {
                options.NumberOfReplicas = 1;
                options.NumberOfShards = 5;
                options.Refresh = Refresh.True;
                options.IndexPrefix = "AElfIndexer";
            });

        }

        public override void OnApplicationInitialization(ApplicationInitializationContext context)
        {
            SeedTestData(context);
        }

        private static void SeedTestData(ApplicationInitializationContext context)
        {
            AsyncHelper.RunSync(async () =>
            {
                using (var scope = context.ServiceProvider.CreateScope())
                {
                    await scope.ServiceProvider
                        .GetRequiredService<IDataSeeder>()
                        .SeedAsync();
                }
            });
        }
    }


}

﻿using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Authorization;
using Volo.Abp.Autofac;
using Volo.Abp.BackgroundJobs;
using Volo.Abp.Data;
using Volo.Abp.Modularity;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.TestBase
{
    [DependsOn(
        typeof(AbpAutofacModule),
        typeof(AbpTestBaseModule),
        typeof(AbpAuthorizationModule))]
    public class AElfEntityMappingTestBaseModule : AbpModule
    {
        public override void PreConfigureServices(ServiceConfigurationContext context)
        {
        }

        public override void ConfigureServices(ServiceConfigurationContext context)
        {
            Configure<AbpBackgroundJobOptions>(options => { options.IsJobExecutionEnabled = false; });

            context.Services.AddAlwaysAllowAuthorization();
            
            context.Services.Configure<ElasticsearchOptions>(options =>
            {
                options.NumberOfReplicas = 1;
                options.NumberOfShards = 6;
                options.Refresh = Refresh.True;
            });
            
            Configure<AElfEntityMappingOptions>(options =>
            {
                options.CollectionPrefix = "AElfEntityMappingTest";
            });

            // TODO: move to AElf.EntityMapping.Tests
            /*context.Services.Configure<ShardInitSettingOptions>(options =>
            {
                options.ShardInitSettings = InitShardInitSettingOptions();
            });*/
            context.Services.Configure<AElfEntityMappingOptions>(options =>
            {
                options.CollectionPrefix = "aelfindexer";
                options.ShardInitSettings = InitShardInitSettingOptions();
            });
        }

        private List<ShardInitSettingDto> InitShardInitSettingOptions()
        {
            ShardInitSettingDto blockIndexDto = new ShardInitSettingDto();
            blockIndexDto.IndexName = "BlockIndex";
            blockIndexDto.ShardChains = new List<ShardChain>()
            {
                new ShardChain()
                {
                    ShardKeys = new List<ShardKey>()
                    {
                        new ShardKey()
                        {
                            Name = "ChainId",
                            Value = "AELF",
                            Step = "",
                            StepType = 0,
                            GroupNo = "0"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "2000",
                            StepType = 1,
                            GroupNo = "0"
                        }
                    }
                },
                new ShardChain()
                {
                    ShardKeys = new List<ShardKey>()
                    {
                        new ShardKey()
                        {
                            Name = "ChainId",
                            Value = "tDVV",
                            Step = "",
                            StepType = 0,
                            GroupNo = "1"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "1000",
                            StepType = 1,
                            GroupNo = "1"
                        }
                    }
                }
            };

            ShardInitSettingDto logEventIndexDto = new ShardInitSettingDto();
            logEventIndexDto.IndexName = "LogEventIndex";
            logEventIndexDto.ShardChains = new List<ShardChain>()
            {
                new ShardChain()
                {
                    ShardKeys = new List<ShardKey>()
                    {
                        new ShardKey()
                        {
                            Name = "ChainId",
                            Value = "AELF",
                            Step = "",
                            StepType = 0,
                            GroupNo = "0"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "2000",
                            StepType = 1,
                            GroupNo = "0"
                        }
                    }
                },
                new ShardChain()
                {
                    ShardKeys = new List<ShardKey>()
                    {
                        new ShardKey()
                        {
                            Name = "ChainId",
                            Value = "tDVV",
                            Step = "",
                            StepType = 0,
                            GroupNo = "1"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "1000",
                            StepType = 1,
                            GroupNo = "1"
                        }
                    }
                }
            };

            return new List<ShardInitSettingDto>()
            {
                blockIndexDto,
                logEventIndexDto
            };

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
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using AElf.EntityMapping.TestBase;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp.AutoMapper;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping;

[DependsOn(typeof(AElfEntityMappingModule),
    typeof(AElfEntityMappingTestBaseModule)
)]
public class AElfEntityMappingTestModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<AbpAutoMapperOptions>(options => { options.AddMaps<AElfEntityMappingTestModule>(); });
        
        Configure<CollectionCreateOptions>(x =>
        {
            x.AddModule(typeof(AElfEntityMappingTestModule));
        });
        Configure<ElasticsearchOptions>(x =>
        {
            x.Uris = new List<string> {"http://localhost:9200"};
        });
        
        context.Services.Configure<AElfEntityMappingOptions>(options =>
        {
            options.CollectionPrefix = "AElfEntityMappingTest";
            options.ShardInitSettings = InitShardInitSettingOptions();
            options.CollectionTailSecondExpireTime = 30;
        });
    }
    
    private List<ShardInitSetting> InitShardInitSettingOptions()
        {
            var blockIndexDto = new ShardInitSetting
            {
                CollectionName = "BlockIndex",
                ShardGroups = new List<ShardGroup>()
                {
                    new ShardGroup()
                    {
                        ShardKeys = new List<ShardKey>()
                        {
                            new ShardKey()
                            {
                                Name = "ChainId",
                                Value = "AELF",
                                Step = "",
                                StepType = StepType.None
                            },
                            new ShardKey()
                            {
                                Name = "BlockHeight",
                                Value = "0",
                                Step = "5",
                                StepType = StepType.Floor
                            }
                        }
                    },
                    new ShardGroup()
                    {
                        ShardKeys = new List<ShardKey>()
                        {
                            new ShardKey()
                            {
                                Name = "ChainId",
                                Value = "tDVV",
                                Step = "",
                                StepType = StepType.None
                            },
                            new ShardKey()
                            {
                                Name = "BlockHeight",
                                Value = "0",
                                Step = "10",
                                StepType = StepType.Floor
                            }
                        }
                    }
                }
            };

            var logEventIndexDto = new ShardInitSetting
            {
                CollectionName = "LogEventIndex",
                ShardGroups = new List<ShardGroup>()
                {
                    new ShardGroup()
                    {
                        ShardKeys = new List<ShardKey>()
                        {
                            new ShardKey()
                            {
                                Name = "ChainId",
                                Value = "AELF",
                                Step = "",
                                StepType = StepType.None
                            },
                            new ShardKey()
                            {
                                Name = "BlockHeight",
                                Value = "0",
                                Step = "2000",
                                StepType = StepType.Floor
                            }
                        }
                    },
                    new ShardGroup()
                    {
                        ShardKeys = new List<ShardKey>()
                        {
                            new ShardKey()
                            {
                                Name = "ChainId",
                                Value = "tDVV",
                                Step = "",
                                StepType = StepType.None
                            },
                            new ShardKey()
                            {
                                Name = "BlockHeight",
                                Value = "0",
                                Step = "1000",
                                StepType = StepType.Floor
                            }
                        }
                    }
                }
            };

            return new List<ShardInitSetting>()
            {
                blockIndexDto,
                logEventIndexDto
            };

        }
}
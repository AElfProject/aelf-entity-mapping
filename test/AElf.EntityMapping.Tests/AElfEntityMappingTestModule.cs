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
        
        context.Services.Configure<AElfEntityMappingOptions>(options =>
        {
            options.CollectionPrefix = "AElfEntityMappingTest";
            options.ShardInitSettings = InitShardInitSettingOptions();
        });
    }
    
    private List<ShardInitSetting> InitShardInitSettingOptions()
        {
            ShardInitSetting blockIndexDto = new ShardInitSetting();
            blockIndexDto.IndexName = "BlockIndex";
            blockIndexDto.ShardGroups = new List<ShardGroup>()
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
                            StepType = StepType.None,
                            GroupNo = "0"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "5",
                            StepType = StepType.Rounding,
                            GroupNo = "0"
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
                            StepType = StepType.None,
                            GroupNo = "1"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "10",
                            StepType = StepType.Rounding,
                            GroupNo = "1"
                        }
                    }
                }
            };

            ShardInitSetting logEventIndexDto = new ShardInitSetting();
            logEventIndexDto.IndexName = "LogEventIndex";
            logEventIndexDto.ShardGroups = new List<ShardGroup>()
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
                            StepType = StepType.None,
                            GroupNo = "0"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "2000",
                            StepType = StepType.Rounding,
                            GroupNo = "0"
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
                            StepType = StepType.None,
                            GroupNo = "1"
                        },
                        new ShardKey()
                        {
                            Name = "BlockHeight",
                            Value = "0",
                            Step = "1000",
                            StepType = StepType.Rounding,
                            GroupNo = "1"
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
using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Options;

public class AElfEntityMappingOptions
{
    public string CollectionPrefix { get; set; }
    public List<ShardInitSettingDto> ShardInitSettings { get; set; }
}
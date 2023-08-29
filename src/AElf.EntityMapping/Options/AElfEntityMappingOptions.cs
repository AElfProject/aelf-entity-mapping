using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Options;

public class AElfEntityMappingOptions
{
    public string CollectionPrefix { get; set; }
    public int CollectionTailSecondExpireTime { get; set; }
    public List<ShardInitSetting> ShardInitSettings { get; set; }
    
    
}
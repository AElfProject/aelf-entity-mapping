using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Options;

public class AElfEntityMappingOptions
{
    public string CollectionPrefix { get; set; }
    public List<ShardInitSetting> ShardInitSettings { get; set; }
    
    public bool IsShardingCollection(Type type)
    {
        if (ShardInitSettings == null)
            return false;
        var options = ShardInitSettings.Find(a => a.CollectionName == type.Name);
        return options != null;
    }
}
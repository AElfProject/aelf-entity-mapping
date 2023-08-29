namespace AElf.EntityMapping.Sharding;

public class ShardInitSetting
{
    public string CollectionName { get; set; }
    public List<ShardGroup> ShardGroups { get; set; }
}

public class ShardGroup
{
    public List<ShardKey> ShardKeys { get; set; }
}

public class ShardKey
{
    public string Name { get; set; }
    public string Value { get; set; }
    public string Step { get; set; }
    public StepType StepType { get; set; }
    
}

public enum StepType
{
    None,
    Floor
}

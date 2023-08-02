namespace AElf.EntityMapping.Sharding;


public class ShardInitSettingOptions
{
    public List<ShardInitSettingDto> ShardInitSettings { get; set; }
}

public class ShardInitSettingDto
{
    public string IndexName { get; set; }
    public List<ShardGroup> ShardGroups { get; set; }
}

// TODO: Rename
public class ShardGroup
{
    public List<ShardKey> ShardKeys { get; set; }
}

public class ShardKey
{
    public string Name { get; set; }
    public string Value { get; set; }
    public string Step { get; set; }
    public int StepType { get; set; }
    
    public string GroupNo { get; set; }
}
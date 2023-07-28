namespace AElf.BaseStorageMapper;


public class ShardInitSettingOptions
{
    public List<ShardInitSettingDto> ShardInitSettings { get; set; }
}

public class ShardInitSettingDto
{
    public string IndexName { get; set; }
    public List<ShardChain> ShardChains { get; set; }
}

public class ShardChain
{
    public List<ShardKey> ShardKeys { get; set; }
}

public class ShardKey
{
    public string Name { get; set; }
    public string Value { get; set; }
    public string Step { get; set; }
    public int StepType { get; set; }
}
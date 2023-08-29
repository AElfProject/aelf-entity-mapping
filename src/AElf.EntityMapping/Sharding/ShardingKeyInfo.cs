namespace AElf.EntityMapping.Sharding;

public class ShardingKeyInfo<TEntity> where TEntity : class
{
    public List<ShardingKey<TEntity>> ShardKeys { get; set; }
    
    public ShardingKeyInfo()
    {
    }
}
public class ShardingKey<TEntity> where TEntity : class
{
    public string ShardKeyName { get; set; }
    
    public string Step { get; set; }
    
    public int Order { get; set; }
    
    public string Value { get; set; }
    
    public StepType StepType { get; set; }
    
    public Func<TEntity, object> Func { get; set; }
    
    public ShardingKey(string keyName, string step, int order, string value, StepType stepType,Func<TEntity, object> func)
    {
        ShardKeyName = keyName;
        Func = func;
        Step = step;
        Order = order;
        Value = value;
        StepType = stepType;
    }
}
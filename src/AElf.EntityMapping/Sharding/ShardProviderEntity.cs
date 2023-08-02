namespace AElf.EntityMapping.Sharding;

public class ShardProviderEntity<TEntity> where TEntity : class
{
    public string SharKeyName { get; set; }
    public string Step { get; set; }
    
    public int Order { get; set; }
    
    public string Value { get; set; }
    
    public string GroupNo { get; set; }
    public Func<TEntity, object> Func { get; set; }
    
    public ShardProviderEntity(string keyName, string step, int order, string value, string groupNo, Func<TEntity, object> func)
    {
        SharKeyName = keyName;
        Func = func;
        Step = step;
        Order = order;
        Value = value;
        GroupNo = groupNo;
    }

}
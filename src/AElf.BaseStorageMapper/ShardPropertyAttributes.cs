namespace AElf.BaseStorageMapper{

[AttributeUsage(AttributeTargets.Property|AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
public class ShardPropertyAttributes : Attribute
{
    public int Order { get; set; }
    public string Name { get; set; }
    public ShardPropertyAttributes(string name, int order)
    {
        Name = name;
        Order = order;
    }
}
}
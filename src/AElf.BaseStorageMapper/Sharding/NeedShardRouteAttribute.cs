namespace AElf.BaseStorageMapper.Sharding;

[AttributeUsage(AttributeTargets.Property|AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
public class NeedShardRouteAttribute : Attribute
{
    public NeedShardRouteAttribute()
    {
    }
}
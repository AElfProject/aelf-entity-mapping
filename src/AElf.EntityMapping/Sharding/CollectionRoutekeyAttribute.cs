namespace AElf.EntityMapping.Sharding;

[AttributeUsage(AttributeTargets.Property|AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
public class CollectionRoutekeyAttribute : Attribute
{
    public CollectionRoutekeyAttribute()
    {
    }
}
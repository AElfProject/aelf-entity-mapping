using AElf.EntityMapping.Entities;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Sharding;

public class CollectionRouteKeyItem<TEntity>
{
    public string FieldName { get; set; }
    // public Type FieldValueType { get; set; }
    // public string FieldValueType { get; set; }
    public string CollectionName { get; set; }
    // public bool IsShardKey { get; set; }
    // public bool IsRouteKey { get; set; }
    public Func<TEntity, string> getRouteKeyValueFunc { get; set; }
}
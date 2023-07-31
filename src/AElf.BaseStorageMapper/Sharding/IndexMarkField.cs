namespace AElf.BaseStorageMapper.Sharding;

public class IndexMarkField
{
    public string FieldName { get; set; }
    public Type FieldValueType { get; set; }
    public string IndexEntityName { get; set; }
    public bool IsShardKey { get; set; }
    public bool IsRouteKey { get; set; }
}
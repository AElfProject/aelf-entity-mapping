namespace AElf.BaseStorageMapper.Elasticsearch.Linq
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property | AttributeTargets.Field)]
    public class NestedAttributes : Attribute
    {
        public string Name { get; set; }

        public NestedAttributes(string name)
        {
            Name = name;
        }
    }
}
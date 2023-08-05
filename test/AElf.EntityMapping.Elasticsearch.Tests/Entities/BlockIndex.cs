using AElf.EntityMapping.Entities;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class BlockIndex:BlockBase,IAElfEntity
{
    public List<string> TransactionIds { get; set; }
    public int LogEventCount { get; set; }
}
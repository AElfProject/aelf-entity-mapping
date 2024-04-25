using AElf.EntityMapping.Entities;
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class BlockIndex : BlockBase, IEntityMappingEntity
{
    public List<string> TransactionIds { get; set; }
    public int LogEventCount { get; set; }
    [Keyword] public string TxnFee { get; set; }
    public FeeIndex Fee { get; set; }
}

public class FeeIndex
{
    [Keyword] public string BlockFee { get; set; }
    public long Fee { get; set; }
}
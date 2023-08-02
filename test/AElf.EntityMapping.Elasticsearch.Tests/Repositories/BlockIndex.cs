using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Sharding;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class BlockIndex : IEntity<string>,IAElfEntity
{
    [Keyword] public string Id { get; set; }

    [Keyword]
    [ShardPropertyAttributes("ChainId", 1)]
    public string ChainId { get; set; }

    [NeedShardRoute]
    [Keyword] public string BlockHash { get; set; }

    [ShardPropertyAttributes("BlockHeight", 2)]
    public long BlockHeight { get; set; }

    [Keyword] public string PreviousBlockHash { get; set; }
    public DateTime BlockTime { get; set; }
    [Keyword] public string SignerPubkey { get; set; }
    [Keyword] public string Signature { get; set; }
    public bool Confirmed { get; set; }
    public Dictionary<string, string> ExtraProperties { get; set; }
    public List<string> TransactionIds { get; set; }
    public int LogEventCount { get; set; }

    public object[] GetKeys()
    {
        return new object[] { Id };
    }
}
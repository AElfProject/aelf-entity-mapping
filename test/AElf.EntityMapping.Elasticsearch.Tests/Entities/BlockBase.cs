using AElf.EntityMapping.Sharding;
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class BlockBase : AElfIndexerEntity<string>, IBlockchainData
{
    [Keyword] public override string Id { get; set; }

    [Keyword]
    [ShardPropertyAttributes("ChainId", 1)]
    public string ChainId { get; set; }

    [CollectionRouteKey] [Keyword] public string BlockHash { get; set; }

    [ShardPropertyAttributes("BlockHeight", 3)]
    public long BlockHeight { get; set; }

    [Keyword] public string PreviousBlockHash { get; set; }
    public DateTime BlockTime { get; set; }
    [Keyword] public string SignerPubkey { get; set; }
    [Keyword] public string Signature { get; set; }

    [ShardPropertyAttributes("Confirmed", 2)]
    public bool Confirmed { get; set; }

    public Dictionary<string, string> ExtraProperties { get; set; }
}
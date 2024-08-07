using Nest;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class Metadata
{
    [Keyword]
    public string ChainId { get; set; }
    public BlockMetadata Block { get; set; }
    public bool IsDeleted { get; set; }
}

public class BlockMetadata
{
    [Keyword]
    public string BlockHash { get; set; }
    public long BlockHeight { get; set; }
    public DateTime BlockTime { get; set; }
}
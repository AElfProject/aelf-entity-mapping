using AElf.EntityMapping.Entities;
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class AccountBalanceEntity: AeFinderEntity<string>, IEntityMappingEntity
{
    [Keyword]public override string Id { get; set; }
    public Metadata Metadata { get; set; } = new ();
    [Keyword] public string Account { get; set; }
    [Keyword] public string Symbol { get; set; }
    public long Amount { get; set; }
}
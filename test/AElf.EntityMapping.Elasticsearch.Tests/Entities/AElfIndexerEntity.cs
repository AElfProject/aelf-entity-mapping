using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Entities;

public class AElfIndexerEntity<TKey>:Entity,IEntity<TKey>
{
    /// <inheritdoc/>
    public virtual TKey Id { get; set; }

    protected AElfIndexerEntity()
    {

    }

    protected AElfIndexerEntity(TKey id)
    {
        Id = id;
    }

    public override object[] GetKeys()
    {
        return new object[] { Id };
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        return $"[ENTITY: {GetType().Name}] Id = {Id}";
    }
}
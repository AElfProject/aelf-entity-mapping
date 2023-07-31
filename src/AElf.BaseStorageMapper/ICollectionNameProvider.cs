using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper;

public interface ICollectionNameProvider<TEntity, TKey>
    where TEntity : class
{
    List<string> GetFullCollectionName(List<CollectionNameCondition> conditions);

    string GetFullCollectionNameById(TKey id);
}

public class CollectionNameCondition
{
    public string Key { get; set; }
    public string Value { get; set; }
    public ConditionType Type { get; set; }

}

public enum ConditionType
{
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}
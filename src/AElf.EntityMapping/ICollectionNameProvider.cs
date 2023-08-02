namespace AElf.EntityMapping;

public interface ICollectionNameProvider<TEntity>
    where TEntity : class
{
    List<string> GetFullCollectionName(List<CollectionNameCondition> conditions);

    string GetFullCollectionNameById<TKey>(TKey id);
}

public class CollectionNameCondition
{
    public string Key { get; set; }
    public object Value { get; set; }
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
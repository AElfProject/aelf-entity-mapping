namespace AElf.EntityMapping;

public interface ICollectionNameProvider<TEntity>
    where TEntity : class
{
    Task<List<string>> GetFullCollectionNameAsync(List<CollectionNameCondition> conditions);

    Task<List<string>> GetFullCollectionNameByEntityAsync(TEntity entity);
    
    Task<List<string>> GetFullCollectionNameByEntityAsync(List<TEntity> entitys);

    Task<string> GetFullCollectionNameByIdAsync<TKey>(TKey id);
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
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.EntityMapping.Sharding;

public interface IShardingKeyProvider<TEntity> where TEntity : class
{
    public void SetShardingKey(int currentNo, int totalCount, string keyName, string step,int order, string value, StepType stepType, Expression body, ReadOnlyCollection<ParameterExpression> parameterExpressions);
    public List<ShardingKeyInfo<TEntity>> GetShardingKeyByEntity();
    
    public Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions);

    public Task<string> GetCollectionName(TEntity entity);
    
    public Task<List<string>> GetCollectionName(List<TEntity> entitys);
    
    public bool IsShardingCollection();

}
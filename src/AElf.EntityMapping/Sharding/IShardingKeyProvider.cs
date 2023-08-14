using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.EntityMapping.Sharding;

public interface IShardingKeyProvider<TEntity> where TEntity : class
{
    public void SetShardingKey(string keyName, string step,int order, string value, string groupNo, StepType stepType, Expression body, ReadOnlyCollection<ParameterExpression> parameterExpressions);
    
    public List<ShardProviderEntity<TEntity>> GetShardingKeyByEntity();
    
    public string GetCollectionName(Dictionary<string,object> conditions);

    public Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions);

    public Task<string> GetCollectionName(TEntity entity);
    
    public Task<List<string>> GetCollectionName(List<TEntity> entitys);
    
    public bool IsShardingCollection();

}
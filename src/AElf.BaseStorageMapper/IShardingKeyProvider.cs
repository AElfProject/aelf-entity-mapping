using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.BaseStorageMapper;

public interface IShardingKeyProvider<TEntity> where TEntity : class, new()
{
    public void SetShardingKey(string keyName, string step,int order, string value, Expression body, ReadOnlyCollection<ParameterExpression> parameterExpressions);

    public ShardProviderEntity<TEntity> GetShardingKeyByEntityAndFieldName(TEntity entity, string fieldName);

    public List<ShardProviderEntity<TEntity>> GetShardingKeyByEntity(Type type);
    
    public string GetCollectionNameForRead(Dictionary<string,object> conditions);
    
    public string GetCollectionNameForWrite(TEntity entity);
    
    public bool IsShardingCollection();

}
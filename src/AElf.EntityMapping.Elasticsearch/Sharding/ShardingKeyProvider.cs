using System.Linq.Expressions;
using System.Reflection;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class, IEntityMappingEntity
{
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly List<ShardInitSetting> _shardInitSettings;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    private List<ShardingKeyInfo<TEntity>> _shardKeyInfoList;
    private readonly Dictionary<string, bool> _existIndexShardDictionary = new Dictionary<string, bool>();
    private readonly Type _type = typeof(TEntity);
    private readonly string _defaultCollectionName;
    private readonly IShardingCollectionTailProvider<TEntity> _shardingCollectionTailProvider;

    public ShardingKeyProvider(IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IElasticsearchClientProvider elasticsearchClientProvider, IShardingCollectionTailProvider<TEntity> shardingCollectionTailProvider,ILogger<ShardingKeyProvider<TEntity>> logger)
    {
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _shardInitSettings = aelfEntityMappingOptions.Value.ShardInitSettings;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _defaultCollectionName = IndexNameHelper.GetDefaultIndexName(_type);
        _shardingCollectionTailProvider = shardingCollectionTailProvider;
        _logger = logger;
    }

    public ShardingKeyProvider()
    {
    }
    private void SetShardKeyInfoList(List<ShardingKey<TEntity>> shardingKeyList)
    {
        shardingKeyList.Sort(new ShardingKeyInfoComparer<TEntity>());
        ShardingKeyInfo<TEntity> shardingKeyInfo = new ShardingKeyInfo<TEntity>()
        {
            ShardKeys = shardingKeyList
        };
        if (_shardKeyInfoList == null)
        {
            _shardKeyInfoList = new List<ShardingKeyInfo<TEntity>>();
        }

        _shardKeyInfoList.Add(shardingKeyInfo);
    }
    
    public List<ShardingKeyInfo<TEntity>> GetShardKeyInfoList()
    {
        if (_shardKeyInfoList == null)
        {
            InitShardProvider();
        }

        return _shardKeyInfoList;
    }
    
    public async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        if (conditions.IsNullOrEmpty())
        {
            return new List<string>();
        }

        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        List<ShardingKeyInfo<TEntity>> filterShardingKeyInfos = shardingKeyInfos;
        List<CollectionNameCondition> filterConditions = new List<CollectionNameCondition>();

        foreach (var condition in conditions)
        {
            var existShardKeyName =
                shardingKeyInfos.Exists(a => a.ShardKeys.Exists(b => b.ShardKeyName == condition.Key));
            if (existShardKeyName)
            {
                filterConditions.Add(condition);
            }
        }

        if (filterConditions.IsNullOrEmpty())
        {
            return new List<string>();
        }

        foreach (var condition in filterConditions)
        {
            filterShardingKeyInfos = filterShardingKeyInfos.FindAll(a => a.ShardKeys.Exists(b =>
                (b.ShardKeyName == condition.Key && b.Value == condition.Value.ToString() && b.StepType == StepType.None) ||
                (b.ShardKeyName == condition.Key && b.StepType == StepType.Floor)));
        }
        _logger.LogDebug(
            "ShardingKeyProvider.GetCollectionNameAsync: conditions: {conditions},ShardingKeyInfo:{ShardingKeyInfo},filterConditions:{filterConditions},filterShardingKeyInfos:{filterShardingKeyInfos}",
            JsonConvert.SerializeObject(conditions), JsonConvert.SerializeObject(shardingKeyInfos.Count),JsonConvert.SerializeObject(filterConditions),JsonConvert.SerializeObject(filterShardingKeyInfos.Count));
        if (filterShardingKeyInfos.IsNullOrEmpty())
        {
            return new List<string>();
        }

        var resultCollectionNames = new List<string>();
        foreach (var shardingKeyInfo in filterShardingKeyInfos)
        {
            long minTail = -1;
            long maxTail = -1;
            var tailPrefix = "";
            List<string> tailPrefixList = new List<string>();
            var equalTypeCollectionName = new List<string>();
            foreach (var shardKey in shardingKeyInfo.ShardKeys)
            {
                if (shardKey.StepType == StepType.None)
                {
                    tailPrefixList.Add(shardKey.Value);
                    continue;
                }
                if(shardKey.StepType == StepType.Floor)
                {
                    tailPrefix = tailPrefixList.JoinAsString(ElasticsearchConstants.CollectionPrefixTailSplit);
                    maxTail = await _shardingCollectionTailProvider.GetShardingCollectionTailAsync(tailPrefix);
                    var shardConditions = conditions.FindAll(a => a.Key == shardKey.ShardKeyName);
                    foreach (var shardCondition in shardConditions)
                    {
                        var conditionType = shardCondition.Type;
                        var tail = (int.Parse(shardCondition.Value.ToString()!) / int.Parse(shardKey.Step));
                        if (conditionType == ConditionType.Equal)
                        {
                            var collectionName = GetCollectionName(_defaultCollectionName, tailPrefix, tail);
                            equalTypeCollectionName.Add(collectionName.ToLower());
                        }

                        if (conditionType == ConditionType.GreaterThan)
                        {
                            minTail = ((int.Parse(shardCondition.Value.ToString()!) + 1) % int.Parse(shardKey.Step) == 0) ? tail + 1 : tail;
                        }

                        if (conditionType == ConditionType.GreaterThanOrEqual)
                        {
                            minTail = tail;
                        }

                        if (conditionType == ConditionType.LessThan)
                        {
                            maxTail = ((int.Parse(shardCondition.Value.ToString()!) - 1) % int.Parse(shardKey.Step) == 0)
                                ? Math.Min(maxTail, tail - 1)
                                : Math.Min(maxTail, tail);
                        }

                        if (conditionType == ConditionType.LessThanOrEqual)
                        {
                            maxTail = Math.Min(maxTail,tail);
                        }
                    }
                }
            }
            tailPrefix = tailPrefixList.JoinAsString(ElasticsearchConstants.CollectionPrefixTailSplit);
            var collectionNames =
                await GetCollectionByRangeAsync(tailPrefix, minTail, maxTail, equalTypeCollectionName);
            resultCollectionNames.AddRange(collectionNames);
        }

        _logger.LogDebug("ShardingKeyProvider.GetCollectionNameAsync-return: conditions: {conditions},resultCollectionNames:{resultCollectionNames}", JsonConvert.SerializeObject(conditions),resultCollectionNames.Distinct().ToList());
        return resultCollectionNames.Distinct().ToList();
    }

    private async Task<bool> CheckCollectionExistAsync(string collectionName)
    {
        var shardIndexNameExist = _existIndexShardDictionary.TryGetValue(collectionName, out var value);
        if (shardIndexNameExist)
        {
            return true;
        }
        else
        {
            var client = _elasticsearchClientProvider.GetClient();
            var exits = await client.Indices.ExistsAsync(collectionName);

            if (exits.Exists)
            {
                _existIndexShardDictionary[collectionName] = true;
                return true;
            }
        }
        return false;
    }
    
    private async Task<List<string>> GetCollectionByRangeAsync(string prefixTail, long min, long max, List<string> equalTypeCollectionNames)
    {
        var indexName = _defaultCollectionName;
        var resultCollectionNames = new List<string>();
        
        if(!equalTypeCollectionNames.IsNullOrEmpty())
        {
            foreach (var equalTypeCollectionName in equalTypeCollectionNames)
            {
               bool exist = await CheckCollectionExistAsync(GetFullName(equalTypeCollectionName));

                if (exist)
                {
                    resultCollectionNames.Add(equalTypeCollectionName);
                }
            }
            return resultCollectionNames;
        }
        
        
        if (min > max)
        {
            return new List<string>() { };
        }

        for (long i = min; i <= max; i++)
        {
            var shardIndexName = GetCollectionName(indexName, prefixTail, i);
            var fullIndexName = GetFullName(shardIndexName);
            var exist =  await CheckCollectionExistAsync(fullIndexName);
            if (exist)
            {
                resultCollectionNames.Add(shardIndexName);
            }
        }
        return resultCollectionNames;
    }

    public async Task<string> GetCollectionNameAsync(TEntity entity)
    {
        var indexName = _defaultCollectionName;
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return indexName.ToLower();
        }

        var (tailPrefix, tail) = GetShardingKeyTail(shardingKeyInfos, entity);
        indexName = GetCollectionName(indexName, tailPrefix, tail);
        //add ShardingCollectionTail
        await _shardingCollectionTailProvider.AddShardingCollectionTailAsync(tailPrefix.ToLower(), tail);
        return indexName;
    }

    public async Task<List<string>> GetCollectionNameAsync(List<TEntity> entities)
    {
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return new List<string>() { _defaultCollectionName.ToLower() };
        }

        List<string> collectionNames = new List<string>();
        //long maxTail = -1;
        var tailPrefix = "";
      
        foreach (var entity in entities)
        {
            var collectionName = _defaultCollectionName;
            (tailPrefix,var tail) = GetShardingKeyTail(shardingKeyInfos, entity);
            collectionName = GetCollectionName(collectionName, tailPrefix, tail);
            collectionNames.Add(collectionName);
            await _shardingCollectionTailProvider.AddShardingCollectionTailAsync(tailPrefix.ToLower(), tail);
        }
        return collectionNames;
    }

    private Tuple<string, long> GetShardingKeyTail(List<ShardingKeyInfo<TEntity>> shardingKeyInfos, TEntity entity)
    {
        var tailPrefix = "";
        var tail = -1;
        foreach (var shardingKeyInfo in shardingKeyInfos)
        {
            List<long> collectionNameTailList = new List<long>();
            List<string> collectionNameTailPrefixList = new List<string>();
            foreach (var shardKey in shardingKeyInfo.ShardKeys)
            {
                var funcValue = shardKey.Func(entity).ToString();
                if (shardKey.StepType == StepType.None)
                {
                    //The field values of entity's sub table must be consistent with the configuration in the sub table configuration file
                    if (funcValue == shardKey.Value)
                    {
                        collectionNameTailPrefixList.Add(shardKey.Value);
                    }
                }
                else
                {
                    tail = int.Parse(funcValue!) / int.Parse(shardKey.Step);
                    collectionNameTailList.Add(tail);
                }
            }

            if ((collectionNameTailPrefixList.Count + collectionNameTailList.Count) == shardingKeyInfo.ShardKeys.Count)
            {
                tailPrefix =
                    collectionNameTailPrefixList.JoinAsString(ElasticsearchConstants.CollectionPrefixTailSplit);
                break;
            }
            else
            {
                collectionNameTailList.Clear();
                collectionNameTailPrefixList.Clear();
            }
        }

        return new Tuple<string, long>(tailPrefix, tail);
    }

    public bool IsShardingCollection()
    {
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return false;
        }

        return true;
    }
    
    private void InitShardProvider()
    {
        List<ShardGroup> shardGroups = _shardInitSettings.Find(a => a.CollectionName == _type.Name)?.ShardGroups;
        var properties = _type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (shardGroups.IsNullOrEmpty())
        {
            return;
        }

        var dic = new Dictionary<string, (int Order, Func<TEntity, object> Func)>();
        foreach (var property in properties)
        {
            var attribute =
                (ShardPropertyAttributes)Attribute.GetCustomAttribute(property, typeof(ShardPropertyAttributes));
            if (attribute != null)
            {
                var propertyExpression = GetPropertyExpression(_type, property.Name);
                var expression = Expression.Lambda<Func<TEntity, object>>(
                    Expression.Convert(propertyExpression.Body, typeof(object)), propertyExpression.Parameters);
                dic[property.Name] = (attribute.Order, expression.Compile());
            }
        }
        
        foreach (var shardGroup in shardGroups)
        {
            var shardingKeyList = new List<ShardingKey<TEntity>>();
            foreach (var shardKey in shardGroup.ShardKeys)
            {
                var key = new ShardingKey<TEntity>(shardKey.Name, shardKey.Step, dic[shardKey.Name].Order, shardKey.Value, shardKey.StepType, dic[shardKey.Name].Func);
                shardingKeyList.Add(key);
            }

            if (shardingKeyList.Count > 0)
            {
                SetShardKeyInfoList(shardingKeyList);
            }
        }
    }

    private LambdaExpression GetPropertyExpression(Type entityType, string propertyName)
    {
        var propertyInfo = entityType.GetProperty(propertyName);

        var parameter = Expression.Parameter(entityType, "entity");

        var propertyAccess = Expression.Property(parameter, propertyInfo);

        var lambda = Expression.Lambda(propertyAccess, parameter);

        return lambda;
    }

    private string GetCollectionName(string typeName, string prefixTail, long tail)
    {
        var indexName = typeName;
        if (tail >= 0)
        {
            if (prefixTail.IsNullOrEmpty())
            {
                indexName = indexName + ElasticsearchConstants.CollectionPrefixTailSplit+ tail;
            }
            else
            {
                indexName = indexName + ElasticsearchConstants.CollectionPrefixTailSplit + prefixTail + ElasticsearchConstants.CollectionPrefixTailSplit + tail;

            }
        }
        else
        {
            if (!prefixTail.IsNullOrEmpty())
            {
                indexName = indexName + ElasticsearchConstants.CollectionPrefixTailSplit + prefixTail;
            }
        }

        return indexName.ToLower();
    }
    private string GetFullName(string collectionName)
    {
        return (_aelfEntityMappingOptions.CollectionPrefix + ElasticsearchConstants.CollectionPrefixSplit + collectionName).ToLower();
    }
}
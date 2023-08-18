using System.Linq.Expressions;
using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class, IEntityMappingEntity
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly List<ShardInitSetting> _shardInitSettings;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    private List<ShardingKeyInfo<TEntity>> _shardKeyInfoList;
    private Dictionary<string, bool> _existIndexShardDictionary = new Dictionary<string, bool>();
    private readonly Type _type = typeof(TEntity);
    private readonly string _defaultCollectionName;

    public ShardingKeyProvider(IOptions<ElasticsearchOptions> indexSettingOptions,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IElasticIndexService elasticIndexService,
        IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingKeyProvider<TEntity>> logger)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _shardInitSettings = aelfEntityMappingOptions.Value.ShardInitSettings;
        _elasticIndexService = elasticIndexService;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
        _defaultCollectionName = IndexNameHelper.GetDefaultIndexName(_type);
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

    private async Task<long> GetShardingCollectionTailAsync(List<CollectionNameCondition> conditions)
    {
        var (total,shardingCollectionTailList) = await GetShardingCollectionTailAsync(new ShardingCollectionTail(){EntityName = _type.Name.ToLower()});
        if (shardingCollectionTailList is null || total == 0)
        {
            return 0;
        }

        List<ShardingKeyInfo<TEntity>> shardingKeyInfoList = GetShardKeyInfoList();
        if (shardingKeyInfoList.IsNullOrEmpty())
        {
            return 0;
        }

        foreach (var condition in conditions)
        {
            var entity = shardingKeyInfoList.Find(a =>
                a.ShardKeys.Find(b => b.ShardKeyName == condition.Key && b.StepType == StepType.None) != null);
            if (entity != null && shardingCollectionTailList != null)
            {
                var shardingCollectionTail =
                    shardingCollectionTailList.Find(a => a.TailPrefix.StartsWith(condition.Value.ToString().ToLower()));
                if (shardingCollectionTail != null)
                {
                    return shardingCollectionTail.Tail;
                }
            }
        }

        return 0;
    }
    
    private async Task<long> GetShardingCollectionTailAsync(string tailPrefix)
    {
        var (total,shardingCollectionTailList) = await GetShardingCollectionTailAsync(new ShardingCollectionTail(){EntityName = _type.Name.ToLower(), TailPrefix = tailPrefix.ToLower()});
        if (shardingCollectionTailList is null || total == 0)
        {
            return 0;
        }

        return shardingCollectionTailList.First().Tail;
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
                (b.ShardKeyName == condition.Key && b.Value == condition.Value && b.StepType == StepType.None) ||
                (b.ShardKeyName == condition.Key && b.StepType == StepType.Floor)));
        }

        if (filterShardingKeyInfos.IsNullOrEmpty())
        {
            return new List<string>();
        }

        var resultCollectionNames = new List<string>();
        foreach (var shardingKeyInfo in filterShardingKeyInfos)
        {
            long minTail = 0;
            long maxTail = 0;
            var tailPrefix = "";
            List<string> tailPrefixList = new List<string>();
            foreach (var shardKey in shardingKeyInfo.ShardKeys)
            {
                var equalTypeCollectionName = new List<string>();
                if (shardKey.StepType == StepType.None)
                {
                    tailPrefixList.Add(shardKey.Value);
                }
                else
                {
                    if (shardKey.StepType != StepType.Floor)
                    {
                        throw new Exception(shardKey.ShardKeyName + "need config StepType equal Floor");
                    }

                    tailPrefix = tailPrefixList.JoinAsString("-");
                    maxTail = await GetShardingCollectionTailAsync(tailPrefix);
                    var shardConditions = conditions.FindAll(a => a.Key == shardKey.ShardKeyName);
                    foreach (var shardCondition in shardConditions)
                    {
                        if (shardCondition != null)
                        {
                            var conditionType = shardCondition.Type;
                            if (conditionType == ConditionType.Equal)
                            {
                                var tail = (int.Parse(shardCondition.Value.ToString()!) / int.Parse(shardKey.Step));
                                var collectionName = _defaultCollectionName + "-" + tailPrefix + "-" + tail;
                                equalTypeCollectionName.Add(collectionName.ToLower());
                            }

                            if (conditionType == ConditionType.GreaterThan)
                            {
                                var value = (int.Parse(shardCondition.Value.ToString()) / int.Parse(shardKey.Step));
                                minTail = ((value + 1) % int.Parse(shardKey.Step) == 0) ? value + 1 : value;
                            }

                            if (conditionType == ConditionType.GreaterThanOrEqual)
                            {
                                minTail = (int.Parse(shardCondition.Value.ToString()) / int.Parse(shardKey.Step));
                            }

                            if (conditionType == ConditionType.LessThan)
                            {
                                var value = (int.Parse(shardCondition.Value.ToString()) / int.Parse(shardKey.Step));
                                maxTail = ((value - 1) % int.Parse(shardKey.Step) == 0)
                                    ? Math.Min(maxTail, value - 1)
                                    : Math.Min(maxTail, value);
                            }

                            if (conditionType == ConditionType.LessThanOrEqual)
                            {
                                maxTail = Math.Min(maxTail,
                                    (int.Parse(shardCondition.Value.ToString()) / int.Parse(shardKey.Step)));
                            }
                        }
                    }

                    var collectionNames =
                        await GetCollectionByRangeAsync(tailPrefix, minTail, maxTail, equalTypeCollectionName);
                    resultCollectionNames.AddRange(collectionNames);
                }
            }
        }

        return resultCollectionNames.Distinct().ToList();
    }

    private async Task<bool> checkCollectionExistAsync(string collectionName)
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
                bool exist = await checkCollectionExistAsync(_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." +equalTypeCollectionName);
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
            var shardIndexName = (indexName + "-" + prefixTail + "-" + i).ToLower();
            var fullIndexName = _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName;
            
            var exist =  await checkCollectionExistAsync(fullIndexName);
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
        var tail = 0;
        var tailPrefix = "";
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return indexName.ToLower();
        }
        
        foreach (var shardKeyInfo in shardingKeyInfos)
        {
            List<long> collectionNameTailList = new List<long>();
            List<string> collectionNameTailPrefixList = new List<string>();
            foreach (var shardKey in shardKeyInfo.ShardKeys)
            {
                if (shardKey.StepType == StepType.None)
                {
                    //The field values of entity's sub table must be consistent with the configuration in the sub table configuration file
                    if (shardKey.Func(entity).ToString() == shardKey.Value)
                    {
                        collectionNameTailPrefixList.Add(shardKey.Value);
                    }
                }
                else
                {
                    if (shardKey.StepType != StepType.Floor)
                    {
                        throw new Exception(shardKey.ShardKeyName + "need config StepType equal Floor");
                    }

                    var value = shardKey.Func(entity);
                    tail = int.Parse(value.ToString()) / int.Parse(shardKey.Step);
                    collectionNameTailList.Add(tail);
                }
            }

            if ((collectionNameTailPrefixList.Count + collectionNameTailList.Count) == shardingKeyInfos.Count)
            {
                tailPrefix = collectionNameTailPrefixList.JoinAsString("-");
                break;
            }
            else
            {
                collectionNameTailList.Clear();
                collectionNameTailPrefixList.Clear();
            }
        }
        indexName = indexName + "-" + tailPrefix + "-" + tail;
        //add ShardingCollectionTail
        await AddShardingCollectionTailAsync(_defaultCollectionName, tailPrefix.ToLower(), tail);
        return indexName.ToLower();
    }

    public async Task<List<string>> GetCollectionNameAsync(List<TEntity> entities)
    {
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardKeyInfoList();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return new List<string>() { _defaultCollectionName.ToLower() };
        }

        List<string> collectionNames = new List<string>();
        long maxShardNo = 0;
        string maxCollectionName = "";
        var tailPrefix = "";
      
        foreach (var entity in entities)
        {
            tailPrefix = "";
            var collectionName = _defaultCollectionName;
            string groupNo = "";
            foreach (var shardingKeyInfo in shardingKeyInfos)
            {
                List<long> collectionNameTailList = new List<long>();
                List<string> collectionNameTailPrefixList = new List<string>();
                foreach (var shardKey in shardingKeyInfo.ShardKeys)
                {
                    if (shardKey.StepType == StepType.None)
                    {
                        //The field values of entity's sub table must be consistent with the configuration in the sub table configuration file
                        if (shardKey.Func(entity).ToString() == shardKey.Value)
                        {
                            collectionNameTailPrefixList.Add(shardKey.Value);
                        }
                    }
                    else
                    {
                        if (shardKey.StepType != StepType.Floor)
                        {
                            throw new Exception(shardKey.ShardKeyName + "need config StepType equal Floor");
                        }

                        var value = shardKey.Func(entity);
                        if (int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardKey.Step) >= maxShardNo)
                        {
                            maxShardNo = int.Parse(value.ToString()) / int.Parse(shardKey.Step);
                        }
                        collectionNameTailList.Add(maxShardNo);
                    }
                }
                if ((collectionNameTailPrefixList.Count + collectionNameTailList.Count) == shardingKeyInfos.Count)
                {
                    tailPrefix = collectionNameTailPrefixList.JoinAsString("-");
                    break;
                }
                else
                {
                    collectionNameTailList.Clear();
                    collectionNameTailPrefixList.Clear();
                }
            }
            collectionName = collectionName + "-" + tailPrefix + "-" + maxShardNo;
            collectionNames.Add(collectionName.ToLower());
        }
        
        await AddShardingCollectionTailAsync(_defaultCollectionName, tailPrefix.ToLower(), maxShardNo);
        return collectionNames;
    }

    public async Task AddOrUpdateAsync(ShardingCollectionTail model)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardingCollectionTail).Name).ToLower();
        var client = _elasticsearchClientProvider.GetClient();
        var exits = client.DocumentExists(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

        if (exits.Exists)
        {
            var result = client.UpdateAsync(DocumentPath<ShardingCollectionTail>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_indexSettingOptions.Refresh));

            if (result.Result.IsValid) return;
            throw new Exception($"Update Document failed at index{indexName} :" + result.Result.ServerError.Error.Reason);
        }
        else
        {
            var result = client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_indexSettingOptions.Refresh));
            if (result.Result.IsValid) return;
            throw new Exception($"Insert Docuemnt failed at index {indexName} :" + result.Result.ServerError.Error.Reason);
        }
    }

    private async Task<Tuple<long, List<ShardingCollectionTail>>> GetShardingCollectionTailAsync(
        ShardingCollectionTail searchDto)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardingCollectionTail).Name).ToLower();
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardingCollectionTail>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(searchDto.EntityName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.TailPrefix).Value(searchDto.TailPrefix)));
        QueryContainer Filter(QueryContainerDescriptor<ShardingCollectionTail> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest> selector = null;
        Expression<Func<ShardingCollectionTail, object>> sortExp = k => k.Tail;
        selector = new Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest>(s =>
            s.Index(indexName).Query(Filter).Sort(st => st.Field(sortExp, SortOrder.Descending)));

        var result = await client.SearchAsync(selector);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetShardingCollectionTailAsync: searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName},result:{JsonConvert.SerializeObject(result)}");
        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }

        return new Tuple<long, List<ShardingCollectionTail>>(result.Total, result.Documents.ToList());
    }

    private async Task AddShardingCollectionTailAsync(string entityName, string keys, long maxShardNo)
    {
        var result = await GetShardingCollectionTailAsync(new ShardingCollectionTail(){EntityName = entityName, TailPrefix = keys});

        List<ShardingCollectionTail> shardingCollectionTailList = result.Item2;
        if (shardingCollectionTailList.IsNullOrEmpty())
        {
            var shardingCollectionTail = new ShardingCollectionTail();
            shardingCollectionTail.EntityName = entityName;
            shardingCollectionTail.TailPrefix = keys;
            shardingCollectionTail.Tail = maxShardNo;
            shardingCollectionTail.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(shardingCollectionTail);
            return;
        }

        var shardingCollection = shardingCollectionTailList.Find(a => a.TailPrefix.Contains(keys));

        if (shardingCollection != null && shardingCollection.Tail < maxShardNo)
        {
            shardingCollection.Tail = maxShardNo;
            await AddOrUpdateAsync(shardingCollection);
            return;
        }
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
}
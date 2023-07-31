using System.Linq.Expressions;
using Remotion.Linq;
using Remotion.Linq.Clauses;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq;

public static class QueryModelExtensions
{
    public static List<CollectionNameCondition> GetCollectionNameConditions(this QueryModel queryModel)
    {
        var conditions = new List<CollectionNameCondition>();
        var whereClauses = queryModel.BodyClauses.OfType<WhereClause>().ToList();
        foreach (var predicate in whereClauses.Select(whereClause => (BinaryExpression)whereClause.Predicate))
        {
            if (predicate.Left is BinaryExpression left)
            {
                Visit(conditions, left);
            }

            if (predicate.Right is BinaryExpression right)
            {
                Visit(conditions, right);
            }
        }

        return conditions;
    }

    private static void Visit(List<CollectionNameCondition> conditions, BinaryExpression expression)
    {
        if (expression.Left.NodeType is ExpressionType.MemberAccess || expression.Right is ConstantExpression)
        {
            var memberExpression = expression.Left as MemberExpression;
            var constantExpression = expression.Right as ConstantExpression;
            
            conditions.Add(new CollectionNameCondition
            {
                Key = memberExpression.Member.Name,
                Value = constantExpression.Value,
                Type = GetConditionType(expression.NodeType)
            });
            return;
        }
        
        if (expression.Left is BinaryExpression left)
        {
            Visit(conditions, left);
        }

        if(expression.Right is BinaryExpression right)
        {
            Visit(conditions, right);
        }
    }

    private static ConditionType GetConditionType(ExpressionType expressionType)
    {
        switch (expressionType)
        {
            case ExpressionType.Equal:
                return ConditionType.Equal;
            case ExpressionType.GreaterThan:
                return ConditionType.GreaterThan;
            case ExpressionType.GreaterThanOrEqual:
                return ConditionType.GreaterThanOrEqual;
            case ExpressionType.LessThan:
                return ConditionType.LessThan;
            case ExpressionType.LessThanOrEqual:
                return ConditionType.LessThanOrEqual;
            default:
                throw new ArgumentOutOfRangeException(nameof(expressionType), expressionType, null);
        }
    }
}
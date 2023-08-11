using System.Linq.Expressions;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;

namespace AElf.EntityMapping.Elasticsearch.Linq;

public static class QueryModelExtensions
{
    public static List<CollectionNameCondition> GetCollectionNameConditions(this QueryModel queryModel)
    {
        var conditions = new List<CollectionNameCondition>();
        VisitQueryModel(conditions, queryModel);
        return conditions;
    }

    private static void VisitQueryModel(List<CollectionNameCondition> conditions, QueryModel queryModel)
    {
        var whereClauses = queryModel.BodyClauses.OfType<WhereClause>().ToList();
        foreach (var predicate in whereClauses.Select(whereClause => (BinaryExpression)whereClause.Predicate))
        {
            if (predicate.Left is BinaryExpression left)
            {
                VisitBinaryExpression(conditions, left);
            }

            if (predicate.Right is BinaryExpression right)
            {
                VisitBinaryExpression(conditions, right);
            }
            
            if (predicate.Left is not BinaryExpression && predicate.Right is not BinaryExpression && predicate is BinaryExpression p)
            {
                VisitBinaryExpression(conditions, p);
            }
        }
    }

    private static void VisitBinaryExpression(List<CollectionNameCondition> conditions, BinaryExpression expression)
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
        
        if (expression.Left is SubQueryExpression leftSub)
        {
            VisitQueryModel(conditions, leftSub.QueryModel);
        }
        else if (expression.Left is BinaryExpression left)
        {
            VisitBinaryExpression(conditions, left);
        }

        if (expression.Right is SubQueryExpression rightSub)
        {
            VisitQueryModel(conditions, rightSub.QueryModel);
        }
        else if(expression.Right is BinaryExpression right)
        {
            VisitBinaryExpression(conditions, right);
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
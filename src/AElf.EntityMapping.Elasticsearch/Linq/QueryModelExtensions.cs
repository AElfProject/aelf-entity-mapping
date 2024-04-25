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
        foreach (var predicate in whereClauses.Select(whereClause => whereClause.Predicate))
        {
            switch (predicate)
            {
                case BinaryExpression binaryExpression:
                    switch (binaryExpression.Left)
                    {
                        case BinaryExpression left:
                            VisitBinaryExpression(conditions, left);
                            break;
                        case SubQueryExpression leftSub:
                            VisitQueryModel(conditions, leftSub.QueryModel);
                            break;
                    }

                    switch (binaryExpression.Right)
                    {
                        case BinaryExpression right:
                            VisitBinaryExpression(conditions, right);
                            break;
                        case SubQueryExpression rightSub:
                            VisitQueryModel(conditions, rightSub.QueryModel);
                            break;
                    }

                    if (binaryExpression.Left is not BinaryExpression
                        && binaryExpression.Left is not SubQueryExpression
                        && binaryExpression.Right is not BinaryExpression
                        && binaryExpression.Right is not SubQueryExpression
                        && binaryExpression is BinaryExpression p)
                    {
                        VisitBinaryExpression(conditions, p);
                    }

                    break;
                case SubQueryExpression subQueryExpression:
                    VisitQueryModel(conditions, subQueryExpression.QueryModel);
                    break;
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
                Key = GetCollectionNameKey(memberExpression),
                Value = constantExpression.Value,
                Type = GetConditionType(expression.NodeType)
            });

            return;
        }

        switch (expression.Left)
        {
            case SubQueryExpression leftSub:
                VisitQueryModel(conditions, leftSub.QueryModel);
                break;
            case BinaryExpression left:
                VisitBinaryExpression(conditions, left);
                break;
        }

        switch (expression.Right)
        {
            case SubQueryExpression rightSub:
                VisitQueryModel(conditions, rightSub.QueryModel);
                break;
            case BinaryExpression right:
                VisitBinaryExpression(conditions, right);
                break;
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

    private static string GetCollectionNameKey(MemberExpression memberExpression)
    {
        var key = memberExpression.Member.Name;
        while (memberExpression.Expression != null)
        {
            memberExpression = memberExpression.Expression as MemberExpression;
            if (memberExpression == null)
            {
                break;
            }

            key = memberExpression.Member.Name + "." + key;
            return key;
        }

        return key;
    }
}
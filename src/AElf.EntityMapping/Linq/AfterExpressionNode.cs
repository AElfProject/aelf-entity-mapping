using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using Remotion.Linq.Clauses;
using Remotion.Linq.Parsing.Structure.IntermediateModel;

namespace AElf.EntityMapping.Linq;

public class AfterExpressionNode : ResultOperatorExpressionNodeBase
{
    private static readonly IEnumerable<MethodInfo> SupportedMethods =
        new ReadOnlyCollection<MethodInfo>((typeof(QueryableExtensions).GetRuntimeMethods()).ToList())
            .Where((Func<MethodInfo, bool>)(mi => mi.Name == "After"));

    public static IEnumerable<MethodInfo> GetSupportedMethods() => SupportedMethods;

    public AfterExpressionNode(MethodCallExpressionParseInfo parseInfo, Expression position)
        : base(parseInfo, null, null)
    {
        Position = position;
    }

    public Expression Position { get; }

    public override Expression Resolve(
        ParameterExpression inputParameter, Expression expressionToBeResolved,
        ClauseGenerationContext clauseGenerationContext)
    {
        return Source.Resolve(inputParameter, expressionToBeResolved, clauseGenerationContext);
    }

    protected override ResultOperatorBase CreateResultOperator(ClauseGenerationContext clauseGenerationContext)
    {
        return new AfterResultOperator(Position);
    }
}
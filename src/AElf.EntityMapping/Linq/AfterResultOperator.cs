using System.Linq.Expressions;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Clauses.StreamedData;

namespace AElf.EntityMapping.Linq;

public class AfterResultOperator : SequenceTypePreservingResultOperatorBase
{
    private Expression _position;

    public AfterResultOperator(Expression position)
    {
        Position = position;
    }

    public Expression Position
    {
        get => _position;
        set
        {
            if (value.Type != typeof(object[]))
            {
                var message =
                    $"The value expression returns '{value.Type}', an expression returning 'System.Object[]' was expected.";
                throw new ArgumentException(message, nameof(value));
            }

            _position = value;
        }
    }

    public object[] GetConstantPosition()
    {
        return GetConstantValueFromExpression<object[]>("position", Position);
    }

    public override ResultOperatorBase Clone(CloneContext cloneContext)
    {
        return new AfterResultOperator(Position);
    }

    public override void TransformExpressions(Func<Expression, Expression> transformation)
    {
        Position = transformation(Position);
    }

    public override string ToString()
    {
        return "After(" + string.Join(",", _position) + ")";
    }

    public override StreamedSequence ExecuteInMemory<T>(StreamedSequence input)
    {
        throw new NotImplementedException();
    }
}
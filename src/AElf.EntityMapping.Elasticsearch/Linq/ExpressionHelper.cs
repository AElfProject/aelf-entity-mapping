using System.Linq.Expressions;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class ExpressionHelper
    {
        public static string GetMemberName<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException("Expression is not a member access expression.", nameof(expression));
            }

            
            var visitor = new MemberNameVisitor();
            visitor.Visit(memberExpression);
            return visitor.MemberName;
        }
        public static string GetMemberNameUnaryExpression<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            var memberExpression = expression.Body as UnaryExpression;
            if (memberExpression == null)
            {
                throw new ArgumentException("Expression is not a member access expression.", nameof(expression));
            }

            MemberExpression memberExp = (MemberExpression)memberExpression.Operand;
            string propertyName = memberExp.Member.Name;
            return propertyName;
        }

        private class MemberNameVisitor : ExpressionVisitor
        {
            public string MemberName { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                MemberName = node.Member.Name;
                return base.VisitMember(node);
            }
        }
    }
}
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class MultiMatchNode : Node
    {
        public string Field { get; set; }
        public object Value { get; set; }

        public MultiMatchNode(string field, object value)
        {
            Field = field;
            Value = value;
        }

        public override QueryContainer Accept(INodeVisitor visitor)
        {
            return visitor.Visit(this);
        }
    }
}
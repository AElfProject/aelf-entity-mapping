using System.Reflection;
using Nest;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class NodeVisitor : INodeVisitor
    {
        public QueryContainer Visit(BoolNode node)
        {
            var queries = node.Children.Select(x => x.Accept(this));
           // var path = node.SubQueryPath;;
           var path = GetAttributePathName(node);;
            if(node.IsSubQuery && !string.IsNullOrEmpty(path))
            {
                return new NestedQuery
                {
                    Path = path,
                    Query = new BoolQuery { Should = queries }
                };
            }
            return new BoolQuery { Should = queries };
        }

        public QueryContainer Visit(OrNode node)
        {
            if (node.IsSubQuery)
            {
                BoolNode boolNode = new BoolNode(node.Optimize());
                boolNode.IsSubQuery = node.IsSubQuery;
                boolNode.SubQueryPath = node.SubQueryPath;
                boolNode.SubQueryFullPath = node.SubQueryFullPath;
                return boolNode.Accept(this);
            }
            return new BoolNode(node.Optimize()).Accept(this);
        }

        public QueryContainer Visit(AndNode node)
        {
            //var path = node.SubQueryPath;
            var path = GetAttributePathName(node);
            var left = node.Left.Accept(this);
            var right = node.Right.Accept(this);
            if(node.IsSubQuery  && !string.IsNullOrEmpty(path))
            {
                return new NestedQuery
                {
                    Path = path,
                    Query = new BoolQuery { Must = new[] { left, right } }
                };
            }
            return new BoolQuery { Must = new[] { left, right } };
        }

        public QueryContainer Visit(NotNode node)
        {
            var child = node.Child.Accept(this);

            return new BoolQuery
            {
                MustNot = new[] { child }
            };
        }

        public QueryContainer Visit(TermNode node)
        {
            var field = getFieldName(node, node.Field);
            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new TermQuery
            {
                Field = node.Field,
                Name = node.Field,
                Value = node.Value
            };
        }

        public QueryContainer Visit(TermsNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new TermsQuery
            {
                Field = node.Field,
                Name = node.Field,
                IsVerbatim = true,
                Terms = node.Values
            };
        }

        public QueryContainer Visit(TermsSetNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new TermsSetQuery
            {
                Field = node.Field,
                Name = node.Field,
                IsVerbatim = true,
                Terms = node.Values,
                MinimumShouldMatchScript = node.Equal
                    ? new InlineScript($"doc['{node.Field}'].length")
                    : new InlineScript("0")
            };
        }

        public QueryContainer Visit(ExistsNode node)
        {
            return new BoolQuery
            {
                Must = new QueryContainer[]
                {
                    new ExistsQuery
                    {
                        Field = node.Field
                    }
                }
            };
        }

        public QueryContainer Visit(NotExistsNode node)
        {
            return new BoolQuery
            {
                MustNot = new QueryContainer[]
                {
                    new ExistsQuery
                    {
                        Field = node.Field
                    }
                }
            };
        }

        public QueryContainer Visit(DateRangeNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new DateRangeQuery
            {
                Field = node.Field,
                Name = node.Field,
                LessThan = node.LessThan,
                LessThanOrEqualTo = node.LessThanOrEqualTo,
                GreaterThan = node.GreaterThan,
                GreaterThanOrEqualTo = node.GreaterThanOrEqualTo
            };
        }

        public QueryContainer Visit(MatchPhraseNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new MatchPhraseQuery
            {
                Field = node.Field,
                Name = node.Field,
                Query = (string)node.Value
            };
        }

        public QueryContainer Visit(NumericRangeNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new NumericRangeQuery
            {
                Field = node.Field,
                Name = node.Field,
                LessThan = node.LessThan,
                LessThanOrEqualTo = node.LessThanOrEqualTo,
                GreaterThan = node.GreaterThan,
                GreaterThanOrEqualTo = node.GreaterThanOrEqualTo
            };
        }

        public QueryContainer Visit(QueryStringNode node)
        {
            return new QueryStringQuery
            {
                Fields = new[] { node.Field },
                Name = node.Field,
                Query = (string)node.Value
            };
        }

        public QueryContainer Visit(MultiMatchNode node)
        {
            var field = getFieldName(node, node.Field);

            if (!string.IsNullOrEmpty(field))
            {
                node.Field = field;
            }
            return new MultiMatchQuery
            {
                Fields = new[] { node.Field },
                Name = node.Field,
                Type = TextQueryType.PhrasePrefix,
                Query = (string)node.Value,
                MaxExpansions = 200
            };
        }
        
        private bool CheckAttribute(string fullClass, string field)
        {
            Type type = Type.GetType(fullClass);
            PropertyInfo propertyInfo = type.GetProperty(char.ToUpper(field[0]) + field.Substring(1));
            NestedAttributes attribute = propertyInfo.GetCustomAttribute<NestedAttributes>();
            if (attribute != null)
            {
                return true;
            }
            return false;
        }
        
        private string GetAttributePathName(string fullClass)
        {
            Type type = Type.GetType(fullClass);
            NestedAttributes attribute = type.GetCustomAttribute<NestedAttributes>();
            if (attribute != null)
            {
                return attribute.Name;
            }
            return "";
        }
        
        public static string GetAttributePathName(Node node)
        {
            if(!node.IsSubQuery || node.SubQueryFullPath.IsNullOrEmpty())
            {
                return "";
            }
            Type type = Type.GetType(node.SubQueryFullPath);
            NestedAttributes attribute = type.GetCustomAttribute<NestedAttributes>();
            var path = "";
            if (attribute != null)
            {
                if (!attribute.Name.IsNullOrEmpty())
                {
                    return attribute.Name;
                }
            }
            return node.SubQueryPath;
        }

        private string getFieldName(Node node, string field)
        {
            var rtnField = field;
            if((!node.IsSubQuery && !node.ParentIsSubQuery) || node.SubQueryFullPath.IsNullOrEmpty())
            {
                return rtnField;
            }
            var path = GetAttributePathName(node.SubQueryFullPath);
            if (!path.IsNullOrEmpty())
            {
                rtnField = path + "." + field;
            }else
            {
                rtnField = node.SubQueryPath + "." + field;
            }

            return rtnField;
        }
    }
}
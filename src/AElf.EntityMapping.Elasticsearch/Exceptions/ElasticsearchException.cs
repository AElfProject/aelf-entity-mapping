using System.Runtime.Serialization;
using Volo.Abp;

namespace AElf.EntityMapping.Elasticsearch.Exceptions
{
    /// <summary>
    /// 
    /// </summary>
    [Serializable]
    public class ElasticsearchException : AbpException
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public ElasticsearchException()
        {

        }

        /// <summary>
        /// Constructor for serializing.
        /// </summary>
        public ElasticsearchException(SerializationInfo serializationInfo, StreamingContext context)
            : base(serializationInfo, context)
        {

        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="message">Exception message</param>
        public ElasticsearchException(string message)
            : base(message)
        {

        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public ElasticsearchException(string message, Exception innerException)
            : base(message, innerException)
        {

        }
    }
}

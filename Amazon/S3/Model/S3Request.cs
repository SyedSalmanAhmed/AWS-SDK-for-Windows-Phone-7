/*******************************************************************************
 *  Copyright 2008-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *  this file except in compliance with the License. A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file.
 *  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 * *****************************************************************************
 *    __  _    _  ___
 *   (  )( \/\/ )/ __)
 *   /__\ \    / \__ \
 *  (_)(_) \/\/  (___/
 *
 *  AWS SDK for .NET
 *  API Version: 2006-03-01
 *
 */

/******************************************************************************
 * Note: Made the following changes:
 * 1. Removed removedHeaders
 * 2. Added byte[] dataStream
 * 
 * Modified by: salman.ahmed@confiz.com
 */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Xml.Serialization;
using Amazon.Runtime;

namespace Amazon.S3.Model
{
    using Map = System.Collections.Generic.IDictionary<Amazon.S3.Model.S3QueryParameter, string>;

    /// <summary>
    /// Base class for all S3 operation requests.
    /// Provides a header collection which can is used to store the request headers.
    /// </summary>
    public class S3Request
    {
        #region Private Members

        private WebHeaderCollection _headers;
        private byte[] _dataStream;
        private long _position;

        // Most requests have less than 10 parameters, so 10 is a safe starting capacity
        // This way, the Map.Add operation will be an O(1) operation
        internal Map parameters = new Dictionary<S3QueryParameter, string>(10);
        
        #endregion

        #region Headers

        /// <summary>
        /// Gets the Headers property.
        /// </summary>
        internal WebHeaderCollection Headers
        {
            get
            {
                if (this._headers == null)
                {
                    this._headers = new WebHeaderCollection();
                }
                return this._headers;
            }
        }

        /// <summary>
        /// Checks if Headers property is set
        /// </summary>
        /// <returns>true if Headers property is set</returns>
        internal bool IsSetHeaders()
        {
            return (this._headers != null && 
                this._headers.Count > 0);
        }

        #endregion

        #region DataStream
        /// <summary>
        /// Gets and sets the InputStream property.
        /// </summary>
        [XmlElementAttribute(ElementName = "DataStream")]
        public byte[] DataStream
        {
            get { return this._dataStream; }
            set { this._dataStream = value; }
        }

        /// <summary>
        /// Sets the DataStream property.
        /// </summary>
        /// <param name="dataStream">DataStream property</param>
        /// <returns>this instance</returns>
        public S3Request WithDataStream(byte[] dataStream)
        {
            this._dataStream = dataStream;
            return this;
        }

        /// <summary>
        /// Checks if InputStream property is set.
        /// </summary>
        /// <returns>true if InputStream property is set.</returns>
        internal bool IsSetDataStream()
        {
            return this._dataStream != null;
        }

        #endregion
 
        #region InputStream

        /// <summary>
        /// Gets and sets the InputStream property.
        /// </summary>
        [XmlElementAttribute(ElementName = "Position")]
        public long Position
        {
            get { return _position; }
            set { _position = value; }
        }

        /// <summary>
        /// Sets the Position property.
        /// </summary>
        /// <param name="inputStream">Position property</param>
        /// <returns>this instance</returns>
        public long WithPosition(long position)
        {
            this._position = position;
            return this._position;
        }

        /// <summary>
        /// Checks if Position property is set.
        /// </summary>
        /// <returns>true if Position property is set.</returns>
        internal bool IsSetPosition()
        {
            return this._position >0;
        }

        #endregion

        #region Request events

        internal event RequestEventHandler BeforeRequestEvent;

        internal S3Request WithBeforeRequestHandler(RequestEventHandler handler)
        {
            BeforeRequestEvent += handler;
            return this;
        }

        internal void FireBeforeRequestEvent(object sender, RequestEventArgs args)
        {
            if (BeforeRequestEvent != null)
                BeforeRequestEvent(sender, args);
        }

        #endregion

        #region Internal properties

        internal string RequestDestinationBucket { get; set; }

        #endregion

        #region Metric properties

        private Guid id = Guid.NewGuid();
        internal Guid Id { get { return this.id; } }


        internal TimeSpan TotalRequestTime { get; set; }
        internal TimeSpan ResponseReadTime { get; set; }
        internal TimeSpan ResponseProcessingTime { get; set; }
        internal TimeSpan ResponseTime { get; set; }
        internal long BytesProcessed { get; set; }

        internal TimeSpan MissingTime
        {
            get
            {
                return (TotalRequestTime - (ResponseReadTime + ResponseProcessingTime + ResponseTime));
            }
        }

        #endregion

        #region Overrides

        public override string ToString()
        {
            string contents = string.Format("S3Request: Type - {0}, ID - {1}, ResponseTime - {2}, ResponseReadTime - {3}, ResponseProcessingTime - {4}, TotalRequestTime - {5}, Unaccounted time - {6}, Bytes processed - {7}",
                this.GetType().FullName,
                this.Id,
                this.ResponseTime,
                this.ResponseReadTime,
                this.ResponseProcessingTime,
                this.TotalRequestTime,
                this.MissingTime,
                this.BytesProcessed);
            return contents;
        }

        #endregion

        #region Virtual methods

        internal virtual bool SupportTimeout
        {
            get { return false; }
        }

        internal virtual bool Expect100Continue
        {
            get { return false; }
        }

        internal virtual void OnRaiseProgressEvent(long incrementTransferred, long transferred, long total)
        {
        }

        #endregion
    }
}
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
 * Note: Following changes made to this class.
 * 1. Removed variable and related content of:
 * a. MD5Digest
 * b. cannedACL
 * c. GenerateMD5Digest
 * d. metaData
 * 
 * Modified by: salman.ahmed@confiz.com
 */

using System;
using System.Xml.Serialization;
using System.Collections.Specialized;

using Amazon.S3.Util;

namespace Amazon.S3.Model
{
    /// <summary>
    /// The PutObjectRequest contains the parameters used for the PutObject operation.
    /// <br />Must set only 1 of ContentBody, InputStream, or FilePath
    /// <br />Required Parameters: BucketName, Key
    /// <br />Optional Parameters: CannedACL, ACL, MD5Digest, GenerateMD5Digest,
    /// ContentType, Metadata, Timeout
    /// </summary>
    public class PutObjectRequest : S3Request
    {
        #region Private Members

        private string bucketName;
        private string key;
        private string filePath;
        private string contentType;
        private string contentBody;
        private int timeout = 0;
        private S3StorageClass storageClass;
        private bool autoCloseStream = true;

        #endregion

        #region Progress Event

        /// <summary>
        /// The event for Put Object progress notifications. All
        /// subscribers will be notified when a new progress
        /// event is raised.
        /// </summary>
        /// <remarks>
        /// Subscribe to this event if you want to receive
        /// put object progress notifications. Here is how:<br />
        /// 1. Define a method with a signature similar to this one:
        /// <code>
        /// private void displayProgress(object sender, PutObjectProgressArgs args)
        /// {
        ///     Console.WriteLine(args);
        /// }
        /// </code>
        /// 2. Add this method to the Put Object Progress Event delegate's invocation list
        /// <code>
        /// PutObjectRequest request = new PutObjectRequest();
        /// request.PutObjectProgressEvent += displayProgress;
        /// </code>
        /// </remarks>
        public event EventHandler<PutObjectProgressArgs> PutObjectProgressEvent;

        /// <summary>
        /// The "handler" will be notified every time a put
        /// object progress event is raised.
        /// </summary>
        /// <param name="handler">A method that consumes the put object progress notification</param>
        /// <returns>this instance of the PutObjectRequest</returns>
        public PutObjectRequest WithSubscriber(EventHandler<PutObjectProgressArgs> handler)
        {
            this.PutObjectProgressEvent += handler;
            return this;
        }

        #endregion

        #region BucketName

        /// <summary>
        /// Gets and sets the BucketName property.
        /// </summary>
        [XmlElementAttribute(ElementName = "BucketName")]
        public string BucketName
        {
            get { return this.bucketName; }
            set { this.bucketName = value; }
        }

        /// <summary>
        /// Sets the BucketName property for this request.
        /// This is the S3 Bucket where the S3 Object you are
        /// creating gets put.
        /// </summary>
        /// <param name="bucketName">The value that BucketName is set to</param>
        /// <returns>the request with the BucketName set</returns>
        public PutObjectRequest WithBucketName(string bucketName)
        {
            this.bucketName = bucketName;
            return this;
        }

        /// <summary>
        /// Checks if BucketName property is set.
        /// </summary>
        /// <returns>true if BucketName property is set.</returns>
        internal bool IsSetBucketName()
        {
            return !System.String.IsNullOrEmpty(this.bucketName);
        }

        #endregion

        #region Key
        /// <summary>
        /// Gets and sets the Key property.
        /// </summary>
        [XmlElementAttribute(ElementName = "Key")]
        public string Key
        {
            get { return this.key; }
            set { this.key = value; }
        }

        /// <summary>
        /// Sets the Key property for this request.
        /// This is the Key for the S3 Object you create.
        /// </summary>
        /// <param name="key">The value that Key is set to</param>
        /// <returns>the request with the Key set</returns>
        public PutObjectRequest WithKey(string key)
        {
            this.key = key;
            return this;
        }

        /// <summary>
        /// Checks if Key property is set.
        /// </summary>
        /// <returns>true if Key property is set.</returns>
        internal bool IsSetKey()
        {
            return !System.String.IsNullOrEmpty(this.key);
        }

        #endregion

        #region FilePath
        /// <summary>
        /// Gets and sets the FilePath property.
        /// </summary>
        [XmlElementAttribute(ElementName = "FilePath")]
        public string FilePath
        {
            get { return this.filePath; }
            set { this.filePath = value; }
        }

        /// <summary>
        /// Sets the FilePath property for this request.
        /// If this is set the request will upload the
        /// specified file to S3. Provide FilePath as an
        /// absolute path.
        /// </summary>
        /// <param name="filePath">The value that FilePath is set to</param>
        /// <returns>the request with the FilePath set</returns>
        public PutObjectRequest WithFilePath(string filePath)
        {
            this.filePath = filePath;
            return this;
        }

        /// <summary>
        /// Checks if FilePath property is set.
        /// </summary>
        /// <returns>true if FilePath property is set.</returns>
        internal bool IsSetFilePath()
        {
            return !System.String.IsNullOrEmpty(this.filePath);
        }

        #endregion


        #region ContentType
        /// <summary>
        /// Gets and sets the ContentType property.
        /// </summary>
        [XmlElementAttribute(ElementName = "ContentType")]
        public string ContentType
        {
            get { return this.contentType; }
            set { this.contentType = value; }
        }

        /// <summary>
        /// Sets the ContentType property for this request.
        /// This property defaults to "binary/octet-stream",
        /// but if you require something else you can set this property.
        /// </summary>
        /// <param name="contentType">the value the ContentType to be set to</param>
        /// <returns>The request with the ContentType set</returns>
        public PutObjectRequest WithContentType(string contentType)
        {
            this.contentType = contentType;
            return this;
        }

        /// <summary>
        /// Checks if ContentType property is set.
        /// </summary>
        /// <returns>true if ContentType property is set.</returns>
        internal bool IsSetContentType()
        {
            return !System.String.IsNullOrEmpty(this.contentType);
        }

        #endregion

        #region ContentBody
        /// <summary>
        /// Gets and sets the ContentBody property.
        /// </summary>
        [XmlElementAttribute(ElementName = "ContentBody")]
        public string ContentBody
        {
            get { return this.contentBody; }
            set
            {
                this.contentBody = value;
                if (value != null)
                {
                    this.contentType = "text/plain";
                }
            }
        }

        /// <summary>
        /// Sets the ContentBody property for this request.
        /// Use this property if you want to upload plaintext to
        /// S3. The ContentBody is the data for your S3 Object.
        /// </summary>
        /// <param name="contentBody">the value the ContentBody to be set to</param>
        /// <returns>The request with the ContentBody set</returns>
        public PutObjectRequest WithContentBody(string contentBody)
        {
            this.contentBody = contentBody;
            this.contentType = "text/plain";
            return this;
        }

        /// <summary>
        /// Checks if ContentBody property is set.
        /// </summary>
        /// <returns>true if ContentBody property is set.</returns>
        internal bool IsSetContentBody()
        {
            return this.contentBody != null;
        }

        #endregion
        
        #region Timeout

        internal override bool SupportTimeout
        {
            get { return true; }
        }

        /// <summary>
        /// Gets and sets of the Timeout property (in milliseconds).
        /// The value of this property is assigned to the
        /// ReadWriteTimeout and Timeout properties of the
        /// HTTPWebRequest object used for S3 PUT Object requests.
        /// </summary>
        /// <remarks>A value less than or equal to 0 will be silently ignored</remarks>
        /// <seealso cref="P:System.Net.HttpWebRequest.ReadWriteTimeout"/>
        /// <seealso cref="P:System.Net.HttpWebRequest.Timeout"/>
        public int Timeout
        {
            get { return this.timeout; }
            set
            {
                if (value > 0)
                {
                    this.timeout = value;
                }
            }
        }

        /// <summary>
        /// Sets the Timeout property (in milliseconds).
        /// Please set the timeout only if you are certain that
        /// the file will not be transferred within the default intervals
        /// for an HttpWebRequest.
        /// </summary>
        /// <param name="timeout">Timeout property</param>
        /// <remarks>A value less than or equal to 0 will be silently ignored</remarks>
        /// <returns>this instance</returns>
        /// <seealso cref="P:System.Net.HttpWebRequest.ReadWriteTimeout"/>
        /// <seealso cref="P:System.Net.HttpWebRequest.Timeout"/>
        public PutObjectRequest WithTimeout(int timeout)
        {
            Timeout = timeout;
            return this;
        }

        #endregion

        #region StorageClass

        /// <summary>
        /// Gets and sets the StorageClass property.
        /// Default: S3StorageClass.Standard. Set this property
        /// only if you want reduced redundancy for this object.
        /// Please refer to 
        /// <see cref="T:Amazon.S3.Model.S3StorageClass"/> for
        /// information on S3 Storage Classes.
        /// </summary>
        public S3StorageClass StorageClass
        {
            get { return this.storageClass; }
            set
            {
                if (value >= S3StorageClass.Standard &&
                    value <= S3StorageClass.ReducedRedundancy)
                {
                    this.storageClass = value;
                }
            }
        }

        /// <summary>
        /// Sets the StorageClass property for this request.
        /// Default: S3StorageClass.Standard. Set this property
        /// only if you want reduced redundancy for this object.
        /// Please refer to 
        /// <see cref="T:Amazon.S3.Model.S3StorageClass"/> for
        /// information on S3 Storage Classes.
        /// </summary>
        /// <param name="sClass">The Storage Class to be set on the object</param>
        /// <returns>The request with the StorageClass set</returns>
        public PutObjectRequest WithStorageClass(S3StorageClass sClass)
        {
            this.storageClass = sClass;
            return this;
        }

        #endregion

        /// <summary>
        /// This method is called by a producer of put object progress
        /// notifications. When called, all the subscribers in the 
        /// invocation list will be called sequentially.
        /// </summary>
        /// <param name="incrementTransferred">The number of bytes transferred since last event</param>
        /// <param name="transferred">The number of bytes transferred</param>
        /// <param name="total">The total number of bytes to be transferred</param>
        internal override void OnRaiseProgressEvent(long incrementTransferred, long transferred, long total)
        {
            // Make a temporary copy of the event to avoid the possibility of
            // a race condition if the last and only subscriber unsubscribes
            // immediately after the null check and before the event is raised.
            EventHandler<PutObjectProgressArgs> handler = PutObjectProgressEvent;
            try
            {
                // Event will be null if there are no subscribers
                if (handler != null)
                {
                    // This automatically calls all subscribers sequentially
                    // http://msdn.microsoft.com/en-us/library/ms173172%28VS.80%29.aspx
                    handler(this, new PutObjectProgressArgs(incrementTransferred, transferred, total));
                }
            }
            catch
            {
            }
        }

        #region AutoCloseStream
        /// <summary>
        /// Gets and sets the AutoCloseStream property. If this value is set to true
        /// then the stream used with this request will be closed when all the content 
        /// is read from the stream.  The property is defaulted to true.
        /// </summary>
        public bool AutoCloseStream
        {
            get { return this.autoCloseStream; }
            set { this.autoCloseStream = value; }
        }

        /// <summary>
        /// Sets the AutoCloseStream property for this request. If this value is set to true
        /// then the stream used with this request will be closed when all the content 
        /// is read from the stream.  The property is defaulted to true.
        /// </summary>
        /// <param name="autoCloseStream">the value the AutoCloseStream to be set to</param>
        /// <returns>The request with the AutoCloseStream set</returns>
        public PutObjectRequest WithAutoCloseStream(bool autoCloseStream)
        {
            this.autoCloseStream = autoCloseStream;
            return this;
        }
        #endregion

        internal override bool Expect100Continue
        {
            get { return true; }
        }
    }

    /// <summary>
    /// Encapsulates the information needed to provide
    /// transfer progress to subscribers of the Put Object
    /// Event.
    /// </summary>
    public class PutObjectProgressArgs : TransferProgressArgs
    {
        /// <summary>
        /// The constructor takes the number of
        /// currently transferred bytes and the
        /// total number of bytes to be transferred
        /// </summary>
        /// <param name="incrementTransferred">The number of bytes transferred since last event</param>
        /// <param name="transferred">The number of bytes transferred</param>
        /// <param name="total">The total number of bytes to be transferred</param>
        public PutObjectProgressArgs(long incrementTransferred, long transferred, long total)
            : base(incrementTransferred, transferred, total)
        {
        }
    }
}
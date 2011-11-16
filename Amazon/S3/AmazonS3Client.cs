/* *****************************************************************************
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
 *  Author(s): Norm Johanson
 */

/*
 * Note: Following changes have been made to this class:
 * 1. To the method "ConvertPutObject", following changes:
 * a. Removed the association with 
 *    i. MD5Digest
 *   ii. Canned ACL
 *  iii. MetaData
 * b. Removed methods:
 *    i. setCannedACLHeader
 *   ii. setMetadataDirectiveHeader
 *  iii. addHttpRange
 *   iv. setMfaHeader
 *   
 * modified by: salman.ahmed@confiz.com
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Serialization;
using System.Xml.Schema;
using System.Xml.Linq;

using Amazon.Util;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.Runtime;
using Amazon.Runtime.Internal.Util;

using Map = System.Collections.Generic.IDictionary<Amazon.S3.Model.S3QueryParameter, string>;

namespace Amazon.S3
{
    public class AmazonS3Client : AmazonS3
    {
        #region Private Members

        private AmazonS3Config config;
        private bool disposed;
        private Type myType;
        private bool ownCredentials;
        private AWSCredentials credentials;

        private static MethodInfo ADD_RANGE_METHODINFO;

        #endregion

        #region Events

        internal event RequestEventHandler BeforeRequestEvent;

        #endregion

        #region Dispose Pattern

        /// <summary>
        /// Implements the Dispose pattern for the AmazonS3Client
        /// </summary>
        /// <param name="fDisposing">Whether this object is being disposed via a call to Dispose
        /// or garbage collected.</param>
        protected virtual void Dispose(bool fDisposing)
        {
            if (!this.disposed)
            {
                if (fDisposing)
                {
                    if (credentials != null)
                    {
                        if (ownCredentials && (credentials is IDisposable))
                        {
                            (credentials as IDisposable).Dispose();
                    }
                        credentials = null;
                }
                }
                this.disposed = true;
            }
        }

        /// <summary>
        /// Disposes of all managed and unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// The destructor for the client class.
        /// </summary>
        ~AmazonS3Client()
        {
            this.Dispose(false);
        }

        #endregion
        
        #region Constructors

        static AmazonS3Client()
        {
            Type t = typeof(HttpWebRequest);
            ADD_RANGE_METHODINFO = t.GetMethod("AddRange", BindingFlags.Instance | BindingFlags.NonPublic, null, new Type[]{typeof(string), typeof(string), typeof(string)}, null);
        }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID and AWS Secret Key
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        public AmazonS3Client(string awsAccessKeyId, string awsSecretAccessKey)
            : this(awsAccessKeyId, awsSecretAccessKey, new AmazonS3Config()) { }

        /// <summary>
        /// Constructs AmazonS3Client with AWS Access Key ID, AWS Secret Key and an
        /// AmazonS3 Configuration object. If the config object's
        /// UseSecureStringForAwsSecretKey is false, the AWS Secret Key
        /// is stored as a clear-text string. Please use this option only
        /// if the application environment doesn't allow the use of SecureStrings.
        /// </summary>
        /// <param name="awsAccessKeyId">AWS Access Key ID</param>
        /// <param name="awsSecretAccessKey">AWS Secret Access Key</param>
        /// <param name="config">The S3 Configuration Object</param>
        public AmazonS3Client(string awsAccessKeyId, string awsSecretAccessKey, AmazonS3Config config)
        {
            this.config = config;
            this.myType = this.GetType();
            if (string.IsNullOrEmpty(awsAccessKeyId))
            {
                this.credentials = null; // anonymous access, no credentials specified                    
            }
            else
            {
                this.credentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey, config.UseSecureStringForAwsSecretKey);
            }
            this.ownCredentials = true;
            }

        /// <summary>
        /// Constructs an AmazonS3Client with AWSCredentials
        /// </summary>
        /// <param name="credentials"></param>
        public AmazonS3Client(AWSCredentials credentials)
            : this(credentials, new AmazonS3Config())
        {
        }

        /// <summary>
        /// Constructs an AmazonS3Client with AWSCredentials and an
        /// Amazon S3 Configuration object
        /// </summary>
        /// <param name="credentials"></param>
        /// <param name="config"></param>
        public AmazonS3Client(AWSCredentials credentials, AmazonS3Config config)
        {
            this.config = config;
            this.myType = this.GetType();
            this.credentials = credentials;
            this.ownCredentials = false;
        }

        #endregion

        #region GetPreSignedURL

        /// <summary>
        /// The GetPreSignedURL operations creates a signed http request.
        /// Query string authentication is useful for giving HTTP or browser
        /// access to resources that would normally require authentication.
        /// When using query string authentication, you create a query,
        /// specify an expiration time for the query, sign it with your
        /// signature, place the data in an HTTP request, and distribute
        /// the request to a user or embed the request in a web page.
        /// A PreSigned URL can be generated for GET, PUT and HEAD
        /// operations on your bucket, keys, and versions.
        /// </summary>
        /// <param name="request">The GetPreSignedUrlRequest that defines the
        /// parameters of the operation.</param>
        /// <returns>A string that is the signed http request.</returns>
        /// <exception cref="T:System.ArgumentException" />
        /// <exception cref="T:System.ArgumentNullException" />
        public string GetPreSignedURL(GetPreSignedUrlRequest request)
        {
            if (credentials == null)
            {
                //throw new AmazonS3Exception("Credentials must be specified, cannot call method anonymously");
            }

            if (request == null)
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The PreSignedUrlRequest specified is null!");
            }

            if (!request.IsSetExpires())
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The Expires Specified is null!");
            }

            if (request.Verb > HttpVerb.PUT)
            {
                throw new ArgumentException(
                    "An Invalid HttpVerb was specified for the GetPreSignedURL request. Valid - GET, HEAD, PUT",
                    S3Constants.RequestParam
                    );
            }

            ConvertGetPreSignedUrl(request);
            return request.parameters[S3QueryParameter.Url];
        }

        #endregion
        
        #region GetObject

        /// <summary>
        /// Initiates the asynchronous execution of the GetObject operation. 
        /// <seealso cref="M:Amazon.S3.AmazonS3.GetObject"/>
        /// </summary>
        /// <param name="request">The GetObjectRequest that defines the parameters of
        /// the operation.</param>
        /// <param name="callback">An AsyncCallback delegate that is invoked when the operation completes.</param>
        /// <param name="state">A user-defined state object that is passed to the callback procedure. Retrieve this object from within the callback procedure using the AsyncState property.</param>
        /// <exception cref="T:System.ArgumentNullException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>An IAsyncResult that can be used to poll or wait for results, or both; 
        /// this value is also needed when invoking EndGetObject.</returns>
        public IAsyncResult BeginGetObject(GetObjectRequest request, AsyncCallback callback, object state)
        {
            return invokeGetObject(request, callback, state, false);
        }

        IAsyncResult invokeGetObject(GetObjectRequest request, AsyncCallback callback, object state, bool synchronzied)
        {
            if (request == null)
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The GetObjectRequest specified is null!");
            }

            if (!request.IsSetBucketName())
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The BucketName specified is null or empty!");
            }
            if (!request.IsSetKey())
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The Key Specified is null or empty!");
            }

            ConvertGetObject(request);
            S3AsyncResult asyncResult = new S3AsyncResult(request, state, callback, synchronzied);
            invoke<GetObjectResponse>(asyncResult);
            return asyncResult;
        }

        #endregion

        #region PutObject

        /// <summary>
        /// Initiates the asynchronous execution of the PutObject operation. 
        /// <seealso cref="M:Amazon.S3.AmazonS3.PutObject"/>
        /// </summary>
        /// <param name="request">The PutObjectRequest that defines the parameters of
        /// the operation.</param>
        /// <param name="callback">An AsyncCallback delegate that is invoked when the operation completes.</param>
        /// <param name="state">A user-defined state object that is passed to the callback procedure. Retrieve this object from within the callback procedure using the AsyncState property.</param>
        /// <exception cref="T:System.ArgumentNullException"></exception>
        /// <exception cref="T:System.Net.WebException"></exception>
        /// <exception cref="T:Amazon.S3.AmazonS3Exception"></exception>
        /// <returns>An IAsyncResult that can be used to poll or wait for results, or both; 
        /// this value is also needed when invoking EndPutObject.</returns>
        public IAsyncResult BeginPutObject(PutObjectRequest request, AsyncCallback callback, object state)
        {
            string methodName = "BeginPutObject";
            AmazonLogger.LogDebugMessage(methodName, "Putting object on S3 with Key = " + request.Key, false);
            return invokePutObject(request, callback, state, false);
        }

        IAsyncResult invokePutObject(PutObjectRequest request, AsyncCallback callback, object state, bool synchronized)
        {
            if (request == null)
            {
                throw new ArgumentNullException(S3Constants.RequestParam, "The PutObjectRequest specified is null!");
            }

            // The BucketName and one of either the Key or the FilePath needs to be set
            if (!request.IsSetBucketName())
            {
                throw new ArgumentException("An S3 Bucket must be specified for S3 PUT object.");
            }

            // Either a stream or file content needs to be provided
            if (!request.IsSetDataStream() && !request.IsSetContentBody())
            {
                throw new ArgumentException(
                    "Please specify either a Filename, provide a FileStream or provide a ContentBody to PUT an object into S3.");
            }

            if (request.IsSetDataStream() && request.IsSetContentBody())
            {
                throw new ArgumentException(
                    "Please specify one of either an Input FileStream or the ContentBody to be PUT as an S3 object.");
            }

            ConvertPutObject(request);
            S3AsyncResult asyncResult = new S3AsyncResult(request, state, callback, synchronized);
            invoke<PutObjectResponse>(asyncResult);
            return asyncResult;
        }

        #endregion
       
        #region Private ConvertXXX Methods
      
        /// <summary>
        /// Convert GetPreSignedUrlRequest to key/value pairs.
        /// </summary>
        /// <param name="request"></param>
        private void ConvertGetPreSignedUrl(GetPreSignedUrlRequest request)
        {
            using (ImmutableCredentials immutableCredentials = credentials.GetCredentials())
            {
                if (immutableCredentials.UseToken)
                {
                    throw new Exception("Cannot get presigned url with temporary credentials");
                }

                Map parameters = request.parameters;

                parameters[S3QueryParameter.Verb] = S3Constants.Verbs[(int)request.Verb];
                parameters[S3QueryParameter.Action] = "GetPreSignedUrl";
                StringBuilder queryStr = new StringBuilder("?AWSAccessKeyId=", 512);
                queryStr.Append(immutableCredentials.AccessKey);

                if (request.IsSetKey())
                {
                    parameters[S3QueryParameter.Key] = request.Key;
                }
                else if (request.Verb == HttpVerb.HEAD)
                {
                    queryStr.Append("&max-keys=0");
                }

                if (request.IsSetContentType())
                {
                    parameters[S3QueryParameter.ContentType] = request.ContentType;
                }

                if (queryStr.Length != 0)
                {
                    queryStr.Append("&");
                }
                queryStr.Append("Expires=");

                string value = Convert.ToInt64((request.Expires.ToUniversalTime() - new DateTime(1970, 1, 1)).TotalSeconds).ToString();
                queryStr.Append(value);
                parameters[S3QueryParameter.Expires] = value;

                StringBuilder queryStrToSign = new StringBuilder();
                if (request.IsSetKey() &&
                    request.IsSetVersionId() &&
                    request.Verb < HttpVerb.PUT)
                {
                    queryStrToSign.AppendFormat("versionId={0}", request.VersionId);
                }

                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_CACHE_CONTROL, request.ResponseHeaderOverrides.CacheControl);
                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_CONTENT_DISPOSITION, request.ResponseHeaderOverrides.ContentDisposition);
                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_CONTENT_ENCODING, request.ResponseHeaderOverrides.ContentEncoding);
                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_CONTENT_LANGUAGE, request.ResponseHeaderOverrides.ContentLanguage);
                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_CONTENT_TYPE, request.ResponseHeaderOverrides.ContentType);
                addParameter(queryStrToSign, ResponseHeaderOverrides.RESPONSE_EXPIRES, request.ResponseHeaderOverrides.Expires);


                if (queryStrToSign.Length > 0)
                {
                    parameters[S3QueryParameter.QueryToSign] = "?" + queryStrToSign.ToString();
                    queryStr.Append("&" + queryStrToSign.ToString());
                }

                parameters[S3QueryParameter.Query] = queryStr.ToString();
                request.RequestDestinationBucket = request.BucketName;
                addS3QueryParameters(request, immutableCredentials);

                // the url needs to be modified so that:
                // 1. The right http protocol is used
                // 2. The auth string is added to the url
                string url = request.parameters[S3QueryParameter.Url];

                // the url's protocol prefix is generated using the config's CommunicationProtocol property. 
                // If the request's protocol differs from that set in the config, make the necessary string replacements.
                if (request.Protocol != config.CommunicationProtocol)
                {
                    switch (config.CommunicationProtocol)
                    {
                        case Protocol.HTTP:
                            url = url.Replace("http://", "https://");
                            break;
                        case Protocol.HTTPS:
                            url = url.Replace("https://", "http://");
                            break;
                    }
                }

                //sign the request
                string toSign = buildSigningString(parameters, request.Headers);
                string auth;
                if (immutableCredentials.UseSecureStringForSecretKey)
                {
                    KeyedHashAlgorithm algorithm = new HMACSHA1();
                    auth = AWSSDKUtils.HMACSign(toSign, immutableCredentials.SecureSecretKey, algorithm);
                }
                else
                {
                    KeyedHashAlgorithm algorithm = new HMACSHA1();
                    auth = AWSSDKUtils.HMACSign(toSign, immutableCredentials.ClearSecretKey, algorithm);
                }
                parameters[S3QueryParameter.Authorization] = auth;

                parameters[S3QueryParameter.Url] = String.Concat(url, "&Signature=", 
                    AmazonS3Util.UrlEncode(request.parameters[S3QueryParameter.Authorization], false));
            }
        }

        /// <summary>
        /// Convert GetObjectRequest to key/value pairs.
        /// </summary>
        /// <param name="request"></param>
        private void ConvertGetObject(GetObjectRequest request)
        {
            Map parameters = request.parameters;
            WebHeaderCollection webHeaders = request.Headers;

            parameters[S3QueryParameter.Verb] = S3Constants.GetVerb;
            parameters[S3QueryParameter.Action] = "GetObject";
            parameters[S3QueryParameter.Key] = request.Key;

            StringBuilder queryStr = new StringBuilder();

            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_CACHE_CONTROL, request.ResponseHeaderOverrides.CacheControl);
            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_CONTENT_DISPOSITION, request.ResponseHeaderOverrides.ContentDisposition);
            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_CONTENT_ENCODING, request.ResponseHeaderOverrides.ContentEncoding);
            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_CONTENT_LANGUAGE, request.ResponseHeaderOverrides.ContentLanguage);
            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_CONTENT_TYPE, request.ResponseHeaderOverrides.ContentType);
            addParameter(queryStr, ResponseHeaderOverrides.RESPONSE_EXPIRES, request.ResponseHeaderOverrides.Expires);


            if (queryStr.Length > 0)
            {
                parameters[S3QueryParameter.Query] = "?" + queryStr.ToString();
                parameters[S3QueryParameter.QueryToSign] = parameters[S3QueryParameter.Query];
            }

            // Add the Timeout parameter
            parameters[S3QueryParameter.RequestTimeout] = request.Timeout.ToString();

            request.RequestDestinationBucket = request.BucketName;
        }

        /// <summary>
        /// Adds the provided key value pair to the request parameter
        /// </summary>
        /// <param name="queryStr"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void addParameter(StringBuilder queryStr, string key, string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                if (queryStr.Length > 0)
                    queryStr.Append("&");

                queryStr.AppendFormat("{0}={1}", key, value);
            }
        }
        
        /// <summary>
        /// Convert PutObjectRequest to key/value pairs.
        /// </summary>
        /// <param name="request"></param>
        protected internal void ConvertPutObject(PutObjectRequest request)
        {
            Map parameters = request.parameters;
            WebHeaderCollection webHeaders = request.Headers;

            parameters[S3QueryParameter.Verb] = S3Constants.PutVerb;
            parameters[S3QueryParameter.Action] = "PutObject";
            parameters[S3QueryParameter.Key] = request.Key;
            

            // Add the Content Type
            if (request.IsSetContentType())
            {
                parameters[S3QueryParameter.ContentType] = request.ContentType;
            }
            else if (request.IsSetFilePath() ||
                request.IsSetKey())
            {
                // Get the extension of the file from the path.
                // Try the key as well.
                string ext = Path.GetExtension(request.FilePath);
                if (String.IsNullOrEmpty(ext) &&
                    request.IsSetKey())
                {
                    ext = Path.GetExtension(request.Key);
                }
                // Use the extension to get the mime-type
                if (!String.IsNullOrEmpty(ext))
                {
                    parameters[S3QueryParameter.ContentType] = AmazonS3Util.MimeTypeFromExtension(ext);
                }
            }

            // Set the Content Length based on whether there is a stream
            if (request.IsSetDataStream())
            {
                parameters[S3QueryParameter.ContentLength] = request.DataStream.Length.ToString();
            }

            if (request.IsSetContentBody())
            {
                // The content length is determined based on the number of bytes
                // needed to represent the content string - check invoke<T>
                parameters[S3QueryParameter.ContentBody] = request.ContentBody;
                // Since a content body was set, let's determine whether a content type was set
                if (!parameters.ContainsKey(S3QueryParameter.ContentType))
                {
                    parameters[S3QueryParameter.ContentType] = AWSSDKUtils.UrlEncodedContent;
                }
            }

            // Add the Timeout parameter
            parameters[S3QueryParameter.RequestTimeout] = request.Timeout.ToString();
            
            // Add the storage class header
            webHeaders[S3Constants.AmzStorageClassHeader] = S3Constants.StorageClasses[(int)request.StorageClass];

            // Finally, add the S3 specific parameters and headers
            request.RequestDestinationBucket = request.BucketName;
        }
    
        #endregion

        #region Private Methods

        T endOperation<T>(IAsyncResult result) where T : class
        {
            S3AsyncResult s3AsyncResult = result as S3AsyncResult;
            if (s3AsyncResult == null)
                return default(T);

            if (!s3AsyncResult.IsCompleted)
            {
                s3AsyncResult.AsyncWaitHandle.WaitOne();
            }

            if (s3AsyncResult.Exception != null)
            {
                AWSSDKUtils.PreserveStackTrace(s3AsyncResult.Exception);
                throw s3AsyncResult.Exception;
            }

            T response = s3AsyncResult.FinalResponse as T;
            s3AsyncResult.FinalResponse = null;
            return response;
        }

        void invoke<T>(S3AsyncResult s3AsyncResult) where T : S3Response, new()
        {
            string methodName = "invoke<T>";
            if (s3AsyncResult.S3Request == null)
            {
                throw new Exception("No request specified for the S3 operation!");
            }

            s3AsyncResult.S3Request.Headers[AWSSDKUtils.UserAgentHeader] = config.UserAgent;

            ProcessRequestHandlers(s3AsyncResult.S3Request);

            ImmutableCredentials immutableCredentials = credentials == null ? null : credentials.GetCredentials();
            try
            {
                addS3QueryParameters(s3AsyncResult.S3Request, immutableCredentials);

                WebHeaderCollection headers = s3AsyncResult.S3Request.Headers;
                Map parameters = s3AsyncResult.S3Request.parameters;

                // if credentials are present (non-anonymous) sign the request
                if (immutableCredentials != null)
                {
                    string toSign = buildSigningString(parameters, headers);
                    string auth;
                    if (immutableCredentials.UseSecureStringForSecretKey)
                    {
                        KeyedHashAlgorithm algorithm = new HMACSHA1();
                        auth = AWSSDKUtils.HMACSign(toSign, immutableCredentials.SecureSecretKey, algorithm);
                    }
                    else
                    {
                        KeyedHashAlgorithm algorithm = new HMACSHA1();
                        auth = AWSSDKUtils.HMACSign(toSign, immutableCredentials.ClearSecretKey, algorithm);
                    }
                    parameters[S3QueryParameter.Authorization] = auth;
                }

                string actionName = parameters[S3QueryParameter.Action];
                string verb = parameters[S3QueryParameter.Verb];

                string message = "Starting request (id "+ s3AsyncResult.S3Request.Id + ") for "+ actionName;
                AmazonLogger.LogDebugMessage(methodName, message, false);

                // Variables that pertain to PUT requests
                byte[] requestData = Encoding.UTF8.GetBytes("");
                long reqDataLen = 0;

                validateVerb(verb);

                if (verb.Equals(S3Constants.PutVerb) || verb.Equals(S3Constants.PostVerb))
                {
                    if (parameters.ContainsKey(S3QueryParameter.ContentBody))
                    {
                        string reqBody = parameters[S3QueryParameter.ContentBody];
                        s3AsyncResult.S3Request.BytesProcessed = reqBody.Length;
                        message = "Request (id " + s3AsyncResult.S3Request.Id + ") body's length [" + reqBody.Length +"]";

                        AmazonLogger.LogDebugMessage(methodName, message, false);
                        requestData = Encoding.UTF8.GetBytes(reqBody);

                        // Since there is a request body, determine the length of the data that will be sent to the server.
                        reqDataLen = requestData.Length;
                        parameters[S3QueryParameter.ContentLength] = reqDataLen.ToString();
                    }

                    if (parameters.ContainsKey(S3QueryParameter.ContentLength))
                    {
                        reqDataLen = Int64.Parse(parameters[S3QueryParameter.ContentLength]);
                    }
                }

                int maxRetries = config.IsSetMaxErrorRetry() ? config.MaxErrorRetry : AWSSDKUtils.DefaultMaxRetry;
                HttpWebRequest request = configureWebRequest(s3AsyncResult.S3Request, reqDataLen, immutableCredentials);
                parameters[S3QueryParameter.RequestAddress] = request.RequestUri.ToString();

                try
                {
                    s3AsyncResult.RequestState = new RequestState(request, parameters, requestData, reqDataLen, s3AsyncResult.S3Request.DataStream);
                    if (reqDataLen > 0)
                    {
                        if (s3AsyncResult.CompletedSynchronously)
                        {
                            this.getRequestStreamCallback<T>(s3AsyncResult);
                        }
                        else
                        {
                            IAsyncResult httpResult = request.BeginGetRequestStream(new AsyncCallback(this.getRequestStreamCallback<T>), s3AsyncResult);
                            if (httpResult.CompletedSynchronously)
                            {
                                if (!s3AsyncResult.RequestState.GetRequestStreamCallbackCalled)
                                {
                                    getRequestStreamCallback<T>(httpResult);
                                }
                                s3AsyncResult.SetCompletedSynchronously(true);
                            }
                        }
                    }
                    else
                    {
                        if (s3AsyncResult.CompletedSynchronously)
                        {
                            this.getResponseCallback<T>(s3AsyncResult);
                        }
                        else
                        {
                            IAsyncResult httpResult = request.BeginGetResponse(new AsyncCallback(this.getResponseCallback<T>), s3AsyncResult);
                            if (httpResult.CompletedSynchronously)
                            {
                                if (!s3AsyncResult.RequestState.GetResponseCallbackCalled)
                                {
                                    getResponseCallback<T>(httpResult);
                                }
                                s3AsyncResult.SetCompletedSynchronously(true);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    AmazonLogger.LogException(methodName + "Error starting async http operation", e);
                    throw;
                }
            }
            finally
            {
                if (immutableCredentials != null)
                {
                    immutableCredentials.Dispose();
                }
            }
        }

        void validateVerb(string verb)
        {
            // The HTTP operation specified has to be one of the operations the Amazon S3 service explicitly supports
            if (!(verb.Equals(S3Constants.PutVerb) ||
                verb.Equals(S3Constants.GetVerb) ||
                verb.Equals(S3Constants.DeleteVerb) ||
                verb.Equals(S3Constants.HeadVerb) ||
                verb.Equals(S3Constants.PostVerb)))
            {
                throw new Exception("Invalid HTTP Operation attempted! Supported operations - GET, HEAD, PUT, DELETE, POST");
            }
        }

        void getRequestStreamCallback<T>(IAsyncResult result) where T : S3Response, new()
        {
            string methodName = "getRequestStreamCallback<T>";
            S3AsyncResult s3AsyncResult;
            if (result is S3AsyncResult)
                s3AsyncResult = result as S3AsyncResult;
            else
                s3AsyncResult = result.AsyncState as S3AsyncResult;

            s3AsyncResult.RequestState.GetRequestStreamCallbackCalled = true;
            try
            {
                RequestState state = s3AsyncResult.RequestState;
                bool shouldRetry = false;
                try
                {
                    Stream requestStream;
                    requestStream = state.WebRequest.EndGetRequestStream(result);

                    using (requestStream)
                    {
                        writeStreamToService(state.ByteData, requestStream);
                    }
                }
                catch (IOException e)
                {
                    shouldRetry = handleIOException(s3AsyncResult.S3Request, s3AsyncResult.RequestState.WebRequest, null, e, s3AsyncResult.RetriesAttempt);
                }

                if (shouldRetry)
                {
                    s3AsyncResult.RetriesAttempt++;
                    handleRetry(s3AsyncResult.S3Request, s3AsyncResult.RequestState.WebRequest, null, s3AsyncResult.OrignalStreamPosition,
                        s3AsyncResult.RetriesAttempt, HttpStatusCode.OK, null);
                    invoke<T>(s3AsyncResult);
                }
                else
                {
                    if (s3AsyncResult.CompletedSynchronously)
                    {
                        this.getResponseCallback<T>(s3AsyncResult);
                    }
                    else
                    {
                        IAsyncResult httpResult = state.WebRequest.BeginGetResponse(new AsyncCallback(this.getResponseCallback<T>), s3AsyncResult);
                        if (httpResult.CompletedSynchronously)
                        {
                            if (!s3AsyncResult.RequestState.GetResponseCallbackCalled)
                            {
                                getResponseCallback<T>(httpResult);
                            }
                            s3AsyncResult.SetCompletedSynchronously(true);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                s3AsyncResult.RequestState.WebRequest.Abort();
                AmazonLogger.LogException(methodName + "Error for GetRequestStream", e);
                s3AsyncResult.Exception = e;

                s3AsyncResult.SignalWaitHandle();
                if (s3AsyncResult.Callback != null)
                    s3AsyncResult.Callback(s3AsyncResult);
            }
        }

        void getResponseCallback<T>(IAsyncResult result) where T : S3Response, new()
        {
            string methodName = "getResponseCallback<T>";
            S3AsyncResult s3AsyncResult;
            if (result is S3AsyncResult)
                s3AsyncResult = result as S3AsyncResult;
            else
                s3AsyncResult = result.AsyncState as S3AsyncResult;

            s3AsyncResult.RequestState.GetResponseCallbackCalled = true;
            bool shouldRetry = false;
            try
            {
                Exception cause = null;
                HttpStatusCode statusCode = HttpStatusCode.OK;
                RequestState state = s3AsyncResult.RequestState;
                HttpWebResponse httpResponse = null;
                T response = null;
                try
                {
                    httpResponse = state.WebRequest.EndGetResponse(result) as HttpWebResponse;

                    TimeSpan lengthOfRequest = DateTime.Now - state.WebRequestStart;
                    s3AsyncResult.S3Request.ResponseTime = lengthOfRequest;
                    shouldRetry = handleHttpResponse<T>(s3AsyncResult.S3Request, state.WebRequest, 
                        httpResponse, s3AsyncResult.RetriesAttempt, lengthOfRequest, 
                        out response, out cause, out statusCode);
                    if (!shouldRetry)
                    {
                        s3AsyncResult.FinalResponse = response;
                    }
                }
                catch (WebException we)
                {
                    shouldRetry = handleHttpWebErrorResponse(s3AsyncResult.S3Request, we, s3AsyncResult.RequestState.WebRequest, httpResponse, out cause, out statusCode);
                    s3AsyncResult.Exception = we;
                }
                catch (IOException e)
                {
                    shouldRetry = handleIOException(s3AsyncResult.S3Request, s3AsyncResult.RequestState.WebRequest, httpResponse, e, s3AsyncResult.RetriesAttempt);
                }

                if (shouldRetry)
                {
                    s3AsyncResult.RetriesAttempt++;
                    WebHeaderCollection respHeaders = null;
                    if (response != null)
                    {
                        respHeaders = response.Headers;
                    }

                    handleRetry(s3AsyncResult.S3Request, s3AsyncResult.RequestState.WebRequest, respHeaders, s3AsyncResult.OrignalStreamPosition,
                        s3AsyncResult.RetriesAttempt, statusCode, cause);
                    invoke<T>(s3AsyncResult);
                }
                else if (cause != null)
                {
                    s3AsyncResult.Exception = cause;
                }
            }
            catch (Exception e)
            {
                AmazonLogger.LogException(methodName + "Error for GetResponse", e);
                s3AsyncResult.Exception = e;
                shouldRetry = false;
            }
            finally
            {
                if (!shouldRetry)
                {
                    s3AsyncResult.SignalWaitHandle();
                    if (s3AsyncResult.Callback != null)
                        s3AsyncResult.Callback(s3AsyncResult);
                }
            }
        }

        /// <summary>
        /// Add authentication related and version parameters
        /// </summary>
        /// <param name="request"></param>
        /// <param name="immutableCredentials"></param>
        void addS3QueryParameters(S3Request request, ImmutableCredentials immutableCredentials)
        {
            if (request == null)
            {
                return;
            }

            string destinationBucket = request.RequestDestinationBucket;

            Map parameters = request.parameters;
            WebHeaderCollection webHeaders = request.Headers;

            if (webHeaders != null)
            {
                webHeaders[S3Constants.AmzDateHeader] = AmazonS3Util.FormattedCurrentTimestamp;
            }

            StringBuilder canonicalResource = new StringBuilder("/", 512);
            if (!String.IsNullOrEmpty(destinationBucket))
            {
                parameters[S3QueryParameter.DestinationBucket] = destinationBucket;
                if (AmazonS3Util.ValidateV2Bucket(destinationBucket))
                {
                    parameters[S3QueryParameter.BucketVersion] = S3Constants.BucketVersions[2];
                }
                else
                {
                    parameters[S3QueryParameter.BucketVersion] = S3Constants.BucketVersions[1];
                }
                canonicalResource.Append(destinationBucket);
                if (!destinationBucket.EndsWith("/"))
                {
                    canonicalResource.Append("/");
                }
            }
            else
            {
                // If there is no destination bucket specified, just use V2.
                parameters[S3QueryParameter.BucketVersion] = S3Constants.BucketVersions[2];
            }

            // The canonical resource doesn't need the query because it is added
            // in the configureWebRequest function directly to the URL
            if (parameters.ContainsKey(S3QueryParameter.Key))
            {
                canonicalResource.Append(parameters[S3QueryParameter.Key]);
            }

            parameters[S3QueryParameter.CanonicalizedResource] = canonicalResource.ToString();

            // Has the user added the Content-Type header to the request?
            string value = webHeaders[AWSSDKUtils.ContentTypeHeader];
            if (!String.IsNullOrEmpty(value))
            {
                // Remove the header from the webHeaders collection
                // and add it to the parameters
                parameters[S3QueryParameter.ContentType] = value;
                //webHeaders.Remove(AWSSDKUtils.ContentTypeHeader);
            }

            // Add token if available
            if (credentials != null && immutableCredentials.UseToken)
            {
                webHeaders[S3Constants.AmzSecurityTokenHeader] = immutableCredentials.Token;
            }

            // Insert the S3 Url into the parameters
            addUrlToParameters(request, config);
        }

        void writeStreamToService(byte[] inputData, Stream requestStream)
        {
            if (inputData != null)
            {
                requestStream.Write(inputData, 0, inputData.Length);
            }
        }
        
        void handleRetry(S3Request userRequest, HttpWebRequest request, WebHeaderCollection respHdrs, long orignalStreamPosition, int retries, HttpStatusCode statusCode, Exception cause)
        {
            string methodName = "handleRetry";
            string actionName = userRequest.parameters[S3QueryParameter.Action];
            string requestAddr = request.RequestUri.ToString();

            if (retries <= this.config.MaxErrorRetry)
            {
                AmazonLogger.LogDebugMessage(methodName, "Retry number " + retries + " for request " + actionName, false);
            }
            pauseOnRetry(retries, this.config.MaxErrorRetry, statusCode, requestAddr, respHdrs, cause);

            // Reset the request so that streams are recreated, removed headers are added back, etc
            prepareRequestForRetry(userRequest, orignalStreamPosition);
        }

        bool handleIOException(S3Request userRequest, HttpWebRequest request, HttpWebResponse httpResponse, IOException e, int retries)
        {
            string methodName = "handleIOException";
            if (isInnerExceptionThreadAbort(e))
                throw e;

            string actionName = userRequest.parameters[S3QueryParameter.Action];
            AmazonLogger.LogException(methodName + "Error making request " + actionName, e);
            if (httpResponse != null)
            {
                httpResponse.Close();
                httpResponse = null;
            }
            // Abort the unsuccessful request
            request.Abort();

            return retries <= this.config.MaxErrorRetry;
        }

        bool isInnerExceptionThreadAbort(Exception e)
        {
            if (e.InnerException is ThreadAbortException)
                return true;
            if (e.InnerException != null)
                return isInnerExceptionThreadAbort(e.InnerException);
            return false;
        }

        bool handleHttpWebErrorResponse(S3Request userRequest, WebException we, HttpWebRequest request, HttpWebResponse httpResponse, out Exception cause, out HttpStatusCode statusCode)
        {
            string methodName = "handleHttpWebErrorResponse";
            WebHeaderCollection respHdrs;
            string actionName = userRequest.parameters[S3QueryParameter.Action];
            string requestAddr = request.RequestUri.ToString();

            AmazonLogger.LogException(methodName + " Error making request " + actionName, we); 
            
            bool shouldRetry;
            using (HttpWebResponse errorResponse = we.Response as HttpWebResponse)
            {
                shouldRetry = processRequestError(actionName, request, we, errorResponse, requestAddr, out respHdrs, typeof(AmazonS3Client), out cause);

                if (httpResponse != null)
                {
                    httpResponse.Close();
                    httpResponse = null;
                }

                // Abort the unsuccessful request regardless of whether we should or shouldn't retry.
                request.Abort();

                if (errorResponse != null)
                {
                    statusCode = errorResponse.StatusCode;
                }
                else
                {
                    statusCode = HttpStatusCode.BadRequest;
                }
            }

            return shouldRetry;
        }

        bool handleHttpResponse<T>(S3Request userRequest, HttpWebRequest request, HttpWebResponse httpResponse, int retries,
            TimeSpan lengthOfRequest, out T response, out Exception cause, out HttpStatusCode statusCode)
            where T : S3Response, new()
        {
            string methodName = "handleHttpResponse<T>";
            response = null;
            cause = null;
            WebHeaderCollection respHdrs = httpResponse.Headers;
            statusCode = httpResponse.StatusCode;
            Map parameters = userRequest.parameters;
            string actionName = parameters[S3QueryParameter.Action];
            string requestAddr = request.RequestUri.ToString();

            bool shouldRetry;
            respHdrs = httpResponse.Headers;
            AmazonLogger.LogDebugMessage(methodName, "Received response for " + actionName + " (id " + userRequest.Id + ") with status code " + httpResponse.StatusCode + " in " + lengthOfRequest + ".", false); 

            statusCode = httpResponse.StatusCode;
            if (!isRedirect(httpResponse))
            {
                // The request submission has completed. Retrieve the response.
                shouldRetry = processRequestResponse<T>(httpResponse, userRequest, myType, out response, out cause);
            }
            else
            {
                shouldRetry = true;

                processRedirect(userRequest, httpResponse);
                AmazonLogger.LogDebugMessage(methodName, "Request for " + actionName + "is being redirect to " + userRequest.parameters[S3QueryParameter.Url] + ".", false);

                pauseOnRetry(retries + 1, this.config.MaxErrorRetry, statusCode, requestAddr, httpResponse.Headers, cause);

                // The HTTPResponse object needs to be closed. Once this is done, the request is gracefully terminated. 
                // Mind you, if this response object is not closed, the client will start getting timeout errors.
                // P.S. This sequence of close-response followed by abort-request 
                // will be repeated through the exception handlers for this try block
                httpResponse.Close();
                httpResponse = null;
                request.Abort();
            }

            return shouldRetry;
        }

        static void processRedirect(S3Request userRequest, HttpWebResponse httpResponse)
        {
            if (httpResponse == null)
            {
                throw new WebException(
                    "The Web Response for a redirected request is null!",
                    WebExceptionStatus.ProtocolError
                    );
            }

            // This is a redirect. Get the URL from the location header
            WebHeaderCollection respHeaders = httpResponse.Headers;
            string value;
            if (!String.IsNullOrEmpty(value = respHeaders["Location"]))
            {
                // This should be the new location for the request
                userRequest.parameters[S3QueryParameter.Url] = value;
            }
        }

        static bool isRedirect(HttpWebResponse httpResponse)
        {
            if (httpResponse == null)
            {
                throw new ArgumentNullException("httpResponse", "Input parameter is null");
            }

            HttpStatusCode statusCode = httpResponse.StatusCode;

            return (statusCode >= HttpStatusCode.MovedPermanently &&
                statusCode < HttpStatusCode.BadRequest);
        }

        /// <summary>
        /// <para>1. Add removed headers back to the request's headers</para>
        /// <para>2. If the InputStream is not-null, reset its position to 0</para>
        /// </summary>
        /// <param name="request"></param>
        /// <param name="orignalStreamPosition"></param>
        void prepareRequestForRetry(S3Request request, long orignalStreamPosition)
        {
            if (request.DataStream != null)
            {
                request.Position = orignalStreamPosition;
            }
        }

        bool processRequestResponse<T>(HttpWebResponse httpResponse, S3Request request, Type t, out T response, out Exception cause)
            where T : S3Response, new()
        {
            string methodName = "processRequestResponse<T>";
            response = default(T);
            cause = null;
            IDictionary<S3QueryParameter, string> parameters = request.parameters;
            string actionName = parameters[S3QueryParameter.Action];
            bool shouldRetry = false;

            if (httpResponse == null)
            {
                throw new WebException(
                    "The Web Response for a successful request is null!",
                    WebExceptionStatus.ProtocolError
                    );
            }

            WebHeaderCollection headerCollection = httpResponse.Headers;
            HttpStatusCode statusCode = httpResponse.StatusCode;
            string responseBody = null;

            try
            {
                if (actionName.Equals("GetObject"))
                {
                    response = new T();
                    Stream respStr = httpResponse.GetResponseStream();
                    request.BytesProcessed = httpResponse.ContentLength;

                    if (parameters.ContainsKey(S3QueryParameter.VerifyChecksum))
                    {
                        try
                        {
                            if (respStr.CanSeek)
                            {
                                response.ResponseStream = respStr;
                            }
                            else
                            {
                                response.ResponseStream = AmazonS3Util.MakeStreamSeekable(respStr);
                            }
                        }
                        catch (Exception)
                        {
                            // Handle this error gracefully by setting the response object
                            // to be null. The outer finally block will catch the exception
                            // and close the httpResponse if the response object is null
                            response = null;
                            throw;
                        }
                    }
                    else
                    {
                        response.ResponseStream = respStr;
                    }
                }
                else
                {
                    using (httpResponse)
                    {
                        DateTime streamRead = DateTime.UtcNow;
                        
                        using (StreamReader reader = new StreamReader(httpResponse.GetResponseStream(), Encoding.UTF8))
                        {                                                        
                            responseBody = reader.ReadToEnd();
                        }
                        request.BytesProcessed = responseBody.Length;
                        responseBody = responseBody.Trim();

                        if (responseBody.EndsWith("/Error>"))
                        {
                            shouldRetry = true;
                        }

                        // Perform response transformation
                        else if (responseBody.EndsWith(">"))
                        {
                            try
                            {
                                // Attempt to deserialize response into <Action> Response type
                                string oldString = " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
                                string newString = ">";

                                responseBody = responseBody.Replace(oldString, newString);

                                oldString = "InitiateMultipartUploadResult";
                                newString = "InitiateMultipartUploadResponse";
                                responseBody = responseBody.Replace(oldString, newString);

                                byte[] byteArray = Encoding.UTF8.GetBytes(responseBody);
                                MemoryStream stream = new MemoryStream(byteArray);

                                XmlSerializer serializer = new XmlSerializer(typeof(T));
                                response = (T)serializer.Deserialize(stream);
                            }
                            catch (Exception ex)
                            {
                                throw ex;
                            }

                            DateTime streamParsed = DateTime.UtcNow;

                            DateTime objectCreated = DateTime.UtcNow;
                            request.ResponseReadTime = streamParsed - streamRead;
                            request.ResponseProcessingTime = objectCreated - streamParsed;
                            AmazonLogger.LogDebugMessage(methodName, "Done reading response stream for request (id "+ request.Id+"). Stream read: "+ request.ResponseReadTime 
                                +". Object create: "+ request.ResponseProcessingTime +". Length of body: " + request.BytesProcessed, false);
                        }
                        else
                        {
                            // We can receive responses that have no response body.
                            // All responses have headers so at a future point,
                            // we "do" attach the headers to the response.
                            response = new T();
                            response.ProcessResponseBody(responseBody);

                            DateTime streamParsed = DateTime.UtcNow;
                            request.ResponseReadTime = streamParsed - streamRead;
                        }
                    }

                    // We are done with our use of the httpResponse object
                    httpResponse = null;
                }
            }
            finally
            {
                if (actionName.Equals("GetObject") &&
                    response != null)
                {
                    // Save the http response object so that it can be closed
                    // gracefully when the GetObjectResponse object is either
                    // garbage-collected or disposed
                    response.httpResponse = httpResponse;
                }
                else if (httpResponse != null)
                {
                    httpResponse.Close();
                    httpResponse = null;
                }

                // Store the headers in the response for all successful service requests
                if (response != null)
                {
                    // Add the header key/value pairs to our <Action> Response type
                    response.Headers = headerCollection;
                    response.ResponseXml = responseBody;
                }
            }

            return shouldRetry;
        }

        private bool processRequestError(string actionName, HttpWebRequest request, WebException we, HttpWebResponse errorResponse, string requestAddr, out WebHeaderCollection respHdrs, Type t, out Exception cause)
        {
            string methodName = "processRequestError";
            bool shouldRetry = false;
            HttpStatusCode statusCode = default(HttpStatusCode);
            string responseBody = null;
            cause = null;

            // Initialize the out parameter to null
            // in case there is no errorResponse
            respHdrs = null;

            if (errorResponse == null)
            {
                AmazonLogger.LogException(methodName + "Error making request " + actionName, we);
                throw we;
            }

            // Set the response headers for future use
            respHdrs = errorResponse.Headers;

            // Obtain the HTTP status code
            statusCode = errorResponse.StatusCode;

            using (StreamReader reader = new StreamReader(errorResponse.GetResponseStream(), Encoding.UTF8))
            {
                responseBody = reader.ReadToEnd();
            }

            if (request.Method.Equals("HEAD"))
            {
                string message = we.Message;
                string errorCode = statusCode.ToString();
                if (statusCode == HttpStatusCode.NotFound)
                {
                    message = "The specified key does not exist";
                    errorCode = "NoSuchKey";
                }

                Exception excep = new Exception(string.Format("Error making request {0}.", actionName));
                AmazonLogger.LogException(methodName, excep);
                throw excep;
            }

            if (statusCode == HttpStatusCode.InternalServerError ||
                statusCode == HttpStatusCode.ServiceUnavailable)
            {
                shouldRetry = true;
                cause = we;
            }
            else
            {
                Exception ex = new Exception(string.Format("Error making request {0}.", actionName));
                AmazonLogger.LogException(methodName, ex);
                throw ex;
            }

            return shouldRetry;
        }

        /// <summary>
        /// <para>Build the Url from the parameters passed in.</para>
        /// <para>Component parts are:</para>
        /// <para>- ServiceURL from the Config</para>
        /// <para>- Bucket</para>
        /// <para>- Key</para>
        /// <para>- urlPrefix</para>
        /// <para>- Query</para>
        /// </summary>
        /// <param name="request"></param>
        /// <param name="config"></param>
        void addUrlToParameters(S3Request request, AmazonS3Config config)
        {
            Map parameters = request.parameters;

            if (parameters.ContainsKey(S3QueryParameter.Url) && !string.IsNullOrEmpty(parameters[S3QueryParameter.Url]))
                return;
            
            if (!config.IsSetServiceURL())
            {
                throw new Exception("The Amazon S3 Service URL is either null or empty");
            }

            string url = config.ServiceURL;

            if (parameters[S3QueryParameter.BucketVersion].Equals(S3Constants.BucketVersions[1]))
            {
                url = String.Concat(url, parameters[S3QueryParameter.CanonicalizedResource]);
            }
            else if (parameters.ContainsKey(S3QueryParameter.DestinationBucket))
            {
                string bucketName = parameters[S3QueryParameter.DestinationBucket];
                if (bucketName.Contains("."))
                    url = String.Concat(url, "/", bucketName, "/");
                else
                    url = String.Concat(bucketName, ".", url, "/");

                if (parameters.ContainsKey(S3QueryParameter.Key))
                {
                    url = String.Concat(url, parameters[S3QueryParameter.Key]);
                }
            }

            string urlPrefix = S3Constants.HTTPS;
            if (config.CommunicationProtocol == Protocol.HTTP)
            {
                urlPrefix = S3Constants.HTTP;
            }
            url = String.Concat(urlPrefix, url);

            // Encode the URL
            url = AmazonS3Util.UrlEncode(url, true);

            if (parameters.ContainsKey(S3QueryParameter.Query))
            {
                url = String.Concat(url, parameters[S3QueryParameter.Query]);
            }

            // Add the Url to the parameters
            parameters[S3QueryParameter.Url] = url;
        }

        /// <summary>
        /// Configure HttpClient with set of defaults as well as configuration from AmazonEC2Config instance
        /// </summary>
        /// <param name="request"></param>
        /// <param name="contentLength"></param>
        /// <param name="credentials"></param>
        /// <returns></returns>
        HttpWebRequest configureWebRequest(S3Request request, long contentLength, ImmutableCredentials credentials)
        {
            WebHeaderCollection headers = request.Headers;
            Map parameters = request.parameters;

            if (!parameters.ContainsKey(S3QueryParameter.Url))
            {
                throw new Exception("The Amazon S3 URL is either null or empty");
            }

            string url = parameters[S3QueryParameter.Url];

            HttpWebRequest httpRequest = WebRequest.Create(url) as HttpWebRequest;

            if (request != null)
            {
                if (parameters.ContainsKey(S3QueryParameter.ContentType))
                {
                    httpRequest.ContentType = parameters[S3QueryParameter.ContentType];
                }

                // Add the AWS Authorization header.
                if (credentials != null && !string.IsNullOrEmpty(credentials.AccessKey))
                {
                    httpRequest.Headers[S3Constants.AuthorizationHeader] = String.Concat(
                        "AWS ", credentials.AccessKey, ":", parameters[S3QueryParameter.Authorization]);
                }

                foreach (string key in headers.AllKeys)
                {
                    httpRequest.Headers[key] = headers[key];
                }
                httpRequest.Method = parameters[S3QueryParameter.Verb];
                httpRequest.AllowAutoRedirect = false;
            }
            return httpRequest;
        }

        /// <summary>
        /// Exponential sleep on failed request
        /// </summary>
        /// <param name="retries"></param>
        /// <param name="maxRetries"></param>
        /// <param name="status"></param>
        /// <param name="requestAddr"></param>
        /// <param name="headers"></param>
        /// <param name="cause"></param>
        void pauseOnRetry(int retries, int maxRetries, HttpStatusCode status, string requestAddr, WebHeaderCollection headers, Exception cause)
        {
            if (retries <= maxRetries)
            {
                int delay = (int)Math.Pow(4, retries) * 100;
                System.Threading.Thread.Sleep(delay);
            }
            else
            {
                throw new Exception(String.Concat("Maximum number of retry attempts reached : ", (retries - 1)));
            }
        }
        
        /// <summary>
        /// Creates a string based on the parameters and encrypts it using key. Returns the encrypted string.
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="webHeaders"></param>
        /// <returns></returns>
        string buildSigningString(IDictionary<S3QueryParameter, string> parameters, WebHeaderCollection webHeaders)
        {
            StringBuilder sb = new StringBuilder("", 256);

            sb.Append(parameters[S3QueryParameter.Verb]);
            sb.Append("\n");

            if (webHeaders != null)
            {
                sb.Append("\n");

                if (parameters.ContainsKey(S3QueryParameter.ContentType))
                {
                    sb.Append(parameters[S3QueryParameter.ContentType]);
                }
                sb.Append("\n");
            }
            else
            {
                // The headers are null, but we still need to append
                // the 2 newlines that are required by S3.
                // Without these, S3 rejects the signature.
                sb.Append("\n\n");
            }

            if (parameters.ContainsKey(S3QueryParameter.Expires))
            {
                sb.Append(parameters[S3QueryParameter.Expires]);
                sb.Append("\n");
            }
            else
            {
                sb.Append("\n");
                sb.Append(buildCanonicalizedHeaders(webHeaders));
            }
            if (parameters.ContainsKey(S3QueryParameter.CanonicalizedResource))
            {
                sb.Append(AmazonS3Util.UrlEncode(parameters[S3QueryParameter.CanonicalizedResource], true));
            }

            string action = parameters[S3QueryParameter.Action];

            if (parameters.ContainsKey(S3QueryParameter.QueryToSign))
            {
                sb.Append(parameters[S3QueryParameter.QueryToSign]);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns a string of all x-amz headers sorted by Ordinal.
        /// </summary>
        /// <param name="headers"></param>
        /// <returns></returns>
        StringBuilder buildCanonicalizedHeaders(WebHeaderCollection headers)
        {
            // Build a sorted list of headers that start with x-amz
            List<string> list = new List<string>(headers.Count);
            foreach (string key in headers.AllKeys)
            {
                string lowerKey = key.ToLower();
                if (lowerKey.StartsWith("x-amz-"))
                {
                    list.Add(lowerKey);
                }
            }
            // Using the recommendations from:
            // http://msdn.microsoft.com/en-us/library/ms973919.aspx
            list.Sort(StringComparer.Ordinal);

            // Create the canonicalized header string to return.
            StringBuilder sb = new StringBuilder(256);
            foreach (string key in list)
            {
                sb.Append(String.Concat(key, ":", headers[key], "\n"));
            }

            return sb;
        }

        private void ProcessRequestHandlers(S3Request request)
        {
            if (request == null) throw new ArgumentNullException("request");

            S3RequestEventArgs args = S3RequestEventArgs.Create(request, config);

            if (request != null)
                request.FireBeforeRequestEvent(this, args);

            if (BeforeRequestEvent != null)
                BeforeRequestEvent(this, args);
        }

        #endregion

    }


    #region Async Classes
    public class S3AsyncResult : IAsyncResult
    {
        #region Private Variables

        private bool _isComplete;
        private bool _completedSynchronously;
        private ManualResetEvent _waitHandle;
        private S3Request _s3Request;
        private AsyncCallback _callback;
        private RequestState _requestState;
        private long _orignalStreamPosition;
        private object _state;
        private int _retiresAttempt;
        private Exception _exception;
        private S3Response _finalResponse;
        private Dictionary<string, object> _parameters;
        private object _lockObj;
        private DateTime _startTime;
        
        #endregion

        #region Internal Variables

        internal S3AsyncResult(S3Request s3Request, object state, AsyncCallback callback, bool completeSynchronized)
        {
            this._s3Request = s3Request;
            this._callback = callback;
            this._state = state;
            this._completedSynchronously = completeSynchronized;

            this._lockObj = new object();

            this._startTime = DateTime.Now;
        }

        internal S3Request S3Request
        {
            get { return this._s3Request; }
            set { this._s3Request = value; }
        }

        internal void SetCompletedSynchronously(bool completedSynchronously)
        {
            this._completedSynchronously = completedSynchronously;
        }

        internal long OrignalStreamPosition
        {
            get { return this._orignalStreamPosition; }
            set { this._orignalStreamPosition = value; }
        }

        internal int RetriesAttempt
        {
            get { return this._retiresAttempt; }
            set { this._retiresAttempt = value; }
        }

        internal AsyncCallback Callback
        {
            get { return this._callback; }
        }

        internal void SignalWaitHandle()
        {
            this._isComplete = true;

            if (this._waitHandle != null)
            {
                this._waitHandle.Set();
            }
        }

        internal object State
        {
            get { return this._state; }
        }

        internal void SetIsComplete(bool isComplete)
        {
            this._isComplete = isComplete;
        }

        internal RequestState RequestState
        {
            get { return this._requestState; }
            set { this._requestState = value; }
        }

        internal Dictionary<string, object> Parameters
        {
            get
            {
                if (this._parameters == null)
                {
                    this._parameters = new Dictionary<string, object>();
                }

                return this._parameters;
            }
        }

        #endregion

        #region Public Properties

        public Exception Exception
        {
            get { return this._exception; }
            set { this._exception = value; }
        }

        public bool CompletedSynchronously
        {
            get { return this._completedSynchronously; }
        }

        public bool IsCompleted
        {
            get { return this._isComplete; }
        }

        public object AsyncState
        {
            get { return this._state; }
        }

        public WaitHandle AsyncWaitHandle
        {
            get
            {
                if (this._waitHandle != null)
                {
                    return this._waitHandle;
                }

                lock (this._lockObj)
                {
                    if (this._waitHandle == null)
                    {
                        this._waitHandle = new ManualResetEvent(this._isComplete);
                    }
                }

                return this._waitHandle;
            }
        }
        
        public S3Response FinalResponse
        {
            get { return this._finalResponse; }
            set
            {
                this._finalResponse = value;
                DateTime endTime = DateTime.Now;
                TimeSpan timeToComplete = endTime - this._startTime;
                this._s3Request.TotalRequestTime = timeToComplete;
                //_logger.InfoFormat("S3 request completed: {0}", this._s3Request);
            }
        }

        #endregion

    }


    class RequestState
    {
        #region Private Variables

        private byte[] _byteData;
        private byte[] _requestData;
        private long _requestDataLength;
        private HttpWebRequest _webRequest;
        private Map _parameters;
        private DateTime _webRequestStart;
        private bool _getRequestStreamCallbackCalled;
        private bool _getResponseCallbackCalled;

        #endregion

        public RequestState(HttpWebRequest webRequest, Map parameters, byte[] requestData, long requestDataLength, byte[] byteData)
        {
            this._webRequest = webRequest;
            this._parameters = parameters;
            this._requestData = requestData;
            this._requestDataLength = requestDataLength;
            this._webRequestStart = DateTime.Now;
            this._byteData = byteData;
        }

        #region Internal Variables

        internal HttpWebRequest WebRequest
        {
            get { return this._webRequest; }
        }

        public byte[] ByteData
        {
            get { return _byteData; }
            set { _byteData = value; }
        }

        internal Map Parameters
        {
            get { return this._parameters; }
        }

        internal byte[] RequestData
        {
            get { return this._requestData; }
        }

        internal long RequestDataLength
        {
            get { return this._requestDataLength; }
        }

        internal DateTime WebRequestStart
        {
            get { return this._webRequestStart; }
            set { this._webRequestStart = value; }
        }

        internal bool GetRequestStreamCallbackCalled
        {
            get { return this._getRequestStreamCallbackCalled; }
            set { this._getRequestStreamCallbackCalled = value; }
        }

        internal bool GetResponseCallbackCalled
        {
            get { return this._getResponseCallbackCalled; }
            set { this._getResponseCallbackCalled = value; }
        }

        #endregion

    }
    #endregion
}

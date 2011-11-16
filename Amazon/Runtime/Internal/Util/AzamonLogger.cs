using System;
using System.Diagnostics;

namespace Amazon.Runtime.Internal.Util
{
    /// <summary>
    /// This class is used to enable logging of messages on Windows Phone 7.x
    /// </summary>
    public class AmazonLogger
    {
        /// <summary>
        /// Logs the message if this is set to be true
        /// </summary>
        private const bool _isDebug = true;

        /// <summary>
        /// The detailed logging of messages is turned off. 
        /// <para>Used to track trivial messages</para>
        /// </summary>
        private const bool _isDetailDebug = false;

        /// <summary>
        /// Logs Debug Message
        /// </summary>
        /// <param name="methodName">Method whose message needs to be marked</param>
        /// <param name="message">Message to be logged</param>
        /// <param name="checkIsDetail">if true mark as detail message, false = log if _isDebug = true</param>
        public static void LogDebugMessage(string methodName, string message, bool checkIsDetail)
        {
            if ((checkIsDetail == false && _isDebug == true) || (checkIsDetail == true && _isDetailDebug == true))
            {
                Debug.WriteLine("{0}, {1} {2}=> {3}", methodName, DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString(), message);
            }
        }

        /// <summary>
        /// Logs all Error Messages
        /// </summary>
        /// <param name="methodName">Method which was to be marked as error</param>
        /// <param name="message">Message to be logged</param>
        public static void LogErrorMessage(string methodName, string message)
        {
            if (_isDebug == true)
            {
                Debug.WriteLine("{0}{1}, {2} {3}=> {4}", Errors.ErrorPrefix, methodName, DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString(), message);
            }
        }

        /// <summary>
        /// Logs all exceptions, checks if exception.Message != null
        /// </summary>
        /// <param name="methodName"></param>
        /// <param name="exceptionMessage">Exception to be logged</param>
        public static void LogException(string methodName, Exception exceptionMessage)
        {
            if (_isDebug == true)
            {
                string errorMessage;
                if (exceptionMessage.Message != null)
                {
                    errorMessage = exceptionMessage.Message.ToString();
                }
                else
                {
                    errorMessage = exceptionMessage.ToString();
                }
                Debug.WriteLine("{0}{1}, {2} {3}=> {4}", Errors.ExceptionPrefix, methodName, DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString(), errorMessage);
            }
            if (exceptionMessage.StackTrace != null)
            {
                LogStackTrace(methodName, exceptionMessage.StackTrace.ToString());
            }
        }

        /// <summary>
        /// Logs the Success Message in proper format
        /// </summary>
        /// <param name="methodName">Method which was to be marked as success</param>
        /// <param name="message">Message to be logged</param>
        public static void LogSuccessMessage(string methodName, string message)
        {
            if (_isDebug == true)
            {
                Debug.WriteLine("{0}{1}, {2} {3}=> {4}", Information.SuccessMessagePrefix, methodName, DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString(), message);
            }
        }

        /// <summary>
        /// Logs the Stack Trace associated with the exception
        /// </summary>
        /// <param name="methodName">Method where exception arose</param>
        /// <param name="stackTrace">Stack trace to be logged</param>
        public static void LogStackTrace(string methodName, string stackTrace)
        {
            Debug.WriteLine("{0}{1}, {2} {3}=> {4}", Errors.ErrorStackTrace, methodName, DateTime.Now.ToLongDateString(), DateTime.Now.ToLongTimeString(), stackTrace);
        }

    }
}


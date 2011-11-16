using System;

namespace Amazon.Runtime.Internal.Util
{
    /// <summary>
    /// Class that contains common Error Messages
    /// </summary>
    public class Errors
    {
        public const string ErrorPrefix = "Error! ";
        public const string ExceptionPrefix = "Exception! ";
        public const string ErrorStackTrace = "Stack Trace: ";

        public const string CredentialsNotLoading = "Some of the credentials couldn't be loaded";
        public const string IncorrectUserNamePwd = "Incorrect username or Password";
        public const string NoNetwork = "Signal strength low or No connectivity. Please try again when network coverage is better";
        public const string TimeSkew = "Upload failed! Please verify device date/time is set correctly and try again";
        public const string TimeOut = "Signal strength low or No connectivity. Please try again when network coverage is better";
        public const string NotImplementedException = "This method is not implemented";
        public const string UsersCouldNotBeLoaded = "None of the users could be loaded. Please try again";
        public const string ProblemLoadingImage = "Problem  Loading Image";
    }
}

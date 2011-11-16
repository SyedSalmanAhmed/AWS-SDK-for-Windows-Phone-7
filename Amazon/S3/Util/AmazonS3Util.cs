﻿/*******************************************************************************
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
 * 1. Removed methods:
 * a. GenerateChecksumForStream
 * b. GenerateChecksumForContent
 * c. CreateHeaderEntry
 * d. SetObjectStorageClass
 * 
 * Modified by: salman.ahmed@confiz.com
 */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

using Amazon.S3.Model;
using Amazon.Util;

namespace Amazon.S3.Util
{
    /// <summary>
    /// Provides utilities used by the Amazon S3 client implementation.
    /// These utilities might be useful to consumers of the Amazon S3
    /// library.
    /// </summary>
    public static class AmazonS3Util
    {
        private static Dictionary<string, string> extensionToMime;

        static AmazonS3Util()
        {
            extensionToMime = extensionToMime = new Dictionary<string, string>(150, StringComparer.InvariantCultureIgnoreCase);

            extensionToMime[".ai"] = "application/postscript";
            extensionToMime[".aif"] = "audio/x-aiff";
            extensionToMime[".aifc"] = "audio/x-aiff";
            extensionToMime[".aiff"] = "audio/x-aiff";
            extensionToMime[".asc"] = "text/plain";
            extensionToMime[".au"] = "audio/basic";
            extensionToMime[".avi"] = "video/x-msvideo";
            extensionToMime[".bcpio"] = "application/x-bcpio";
            extensionToMime[".bin"] = "application/octet-stream";
            extensionToMime[".c"] = "text/plain";
            extensionToMime[".cc"] = "text/plain";
            extensionToMime[".ccad"] = "application/clariscad";
            extensionToMime[".cdf"] = "application/x-netcdf";
            extensionToMime[".class"] = "application/octet-stream";
            extensionToMime[".cpio"] = "application/x-cpio";
            extensionToMime[".cpp"] = "text/plain";
            extensionToMime[".cpt"] = "application/mac-compactpro";
            extensionToMime[".cs"] = "text/plain";
            extensionToMime[".csh"] = "application/x-csh";
            extensionToMime[".css"] = "text/css";
            extensionToMime[".dcr"] = "application/x-director";
            extensionToMime[".dir"] = "application/x-director";
            extensionToMime[".dms"] = "application/octet-stream";
            extensionToMime[".doc"] = "application/msword";
            extensionToMime[".docx"] = "application/msword";
            extensionToMime[".dot"] = "application/msword";
            extensionToMime[".drw"] = "application/drafting";
            extensionToMime[".dvi"] = "application/x-dvi";
            extensionToMime[".dwg"] = "application/acad";
            extensionToMime[".dxf"] = "application/dxf";
            extensionToMime[".dxr"] = "application/x-director";
            extensionToMime[".eps"] = "application/postscript";
            extensionToMime[".etx"] = "text/x-setext";
            extensionToMime[".exe"] = "application/octet-stream";
            extensionToMime[".ez"] = "application/andrew-inset";
            extensionToMime[".f"] = "text/plain";
            extensionToMime[".f90"] = "text/plain";
            extensionToMime[".fli"] = "video/x-fli";
            extensionToMime[".gif"] = "image/gif";
            extensionToMime[".gtar"] = "application/x-gtar";
            extensionToMime[".gz"] = "application/x-gzip";
            extensionToMime[".h"] = "text/plain";
            extensionToMime[".hdf"] = "application/x-hdf";
            extensionToMime[".hh"] = "text/plain";
            extensionToMime[".hqx"] = "application/mac-binhex40";
            extensionToMime[".htm"] = "text/html";
            extensionToMime[".html"] = "text/html";
            extensionToMime[".ice"] = "x-conference/x-cooltalk";
            extensionToMime[".ief"] = "image/ief";
            extensionToMime[".iges"] = "model/iges";
            extensionToMime[".igs"] = "model/iges";
            extensionToMime[".ips"] = "application/x-ipscript";
            extensionToMime[".ipx"] = "application/x-ipix";
            extensionToMime[".jpe"] = "image/jpeg";
            extensionToMime[".jpeg"] = "image/jpeg";
            extensionToMime[".jpg"] = "image/jpeg";
            extensionToMime[".js"] = "application/x-javascript";
            extensionToMime[".kar"] = "audio/midi";
            extensionToMime[".latex"] = "application/x-latex";
            extensionToMime[".lha"] = "application/octet-stream";
            extensionToMime[".lsp"] = "application/x-lisp";
            extensionToMime[".lzh"] = "application/octet-stream";
            extensionToMime[".m"] = "text/plain";
            extensionToMime[".man"] = "application/x-troff-man";
            extensionToMime[".me"] = "application/x-troff-me";
            extensionToMime[".mesh"] = "model/mesh";
            extensionToMime[".mid"] = "audio/midi";
            extensionToMime[".midi"] = "audio/midi";
            extensionToMime[".mime"] = "www/mime";
            extensionToMime[".mov"] = "video/quicktime";
            extensionToMime[".movie"] = "video/x-sgi-movie";
            extensionToMime[".mp2"] = "audio/mpeg";
            extensionToMime[".mp3"] = "audio/mpeg";
            extensionToMime[".mpe"] = "video/mpeg";
            extensionToMime[".mpeg"] = "video/mpeg";
            extensionToMime[".mpg"] = "video/mpeg";
            extensionToMime[".mpga"] = "audio/mpeg";
            extensionToMime[".ms"] = "application/x-troff-ms";
            extensionToMime[".msi"] = "application/x-ole-storage";
            extensionToMime[".msh"] = "model/mesh";
            extensionToMime[".nc"] = "application/x-netcdf";
            extensionToMime[".oda"] = "application/oda";
            extensionToMime[".pbm"] = "image/x-portable-bitmap";
            extensionToMime[".pdb"] = "chemical/x-pdb";
            extensionToMime[".pdf"] = "application/pdf";
            extensionToMime[".pgm"] = "image/x-portable-graymap";
            extensionToMime[".pgn"] = "application/x-chess-pgn";
            extensionToMime[".png"] = "image/png";
            extensionToMime[".pnm"] = "image/x-portable-anymap";
            extensionToMime[".pot"] = "application/mspowerpoint";
            extensionToMime[".ppm"] = "image/x-portable-pixmap";
            extensionToMime[".pps"] = "application/mspowerpoint";
            extensionToMime[".ppt"] = "application/mspowerpoint";
            extensionToMime[".ppz"] = "application/mspowerpoint";
            extensionToMime[".pre"] = "application/x-freelance";
            extensionToMime[".prt"] = "application/pro_eng";
            extensionToMime[".ps"] = "application/postscript";
            extensionToMime[".qt"] = "video/quicktime";
            extensionToMime[".ra"] = "audio/x-realaudio";
            extensionToMime[".ram"] = "audio/x-pn-realaudio";
            extensionToMime[".ras"] = "image/cmu-raster";
            extensionToMime[".rgb"] = "image/x-rgb";
            extensionToMime[".rm"] = "audio/x-pn-realaudio";
            extensionToMime[".roff"] = "application/x-troff";
            extensionToMime[".rpm"] = "audio/x-pn-realaudio-plugin";
            extensionToMime[".rtf"] = "text/rtf";
            extensionToMime[".rtx"] = "text/richtext";
            extensionToMime[".scm"] = "application/x-lotusscreencam";
            extensionToMime[".set"] = "application/set";
            extensionToMime[".sgm"] = "text/sgml";
            extensionToMime[".sgml"] = "text/sgml";
            extensionToMime[".sh"] = "application/x-sh";
            extensionToMime[".shar"] = "application/x-shar";
            extensionToMime[".silo"] = "model/mesh";
            extensionToMime[".sit"] = "application/x-stuffit";
            extensionToMime[".skd"] = "application/x-koan";
            extensionToMime[".skm"] = "application/x-koan";
            extensionToMime[".skp"] = "application/x-koan";
            extensionToMime[".skt"] = "application/x-koan";
            extensionToMime[".smi"] = "application/smil";
            extensionToMime[".smil"] = "application/smil";
            extensionToMime[".snd"] = "audio/basic";
            extensionToMime[".sol"] = "application/solids";
            extensionToMime[".spl"] = "application/x-futuresplash";
            extensionToMime[".src"] = "application/x-wais-source";
            extensionToMime[".step"] = "application/STEP";
            extensionToMime[".stl"] = "application/SLA";
            extensionToMime[".stp"] = "application/STEP";
            extensionToMime[".sv4cpio"] = "application/x-sv4cpio";
            extensionToMime[".sv4crc"] = "application/x-sv4crc";
            extensionToMime[".swf"] = "application/x-shockwave-flash";
            extensionToMime[".t"] = "application/x-troff";
            extensionToMime[".tar"] = "application/x-tar";
            extensionToMime[".tcl"] = "application/x-tcl";
            extensionToMime[".tex"] = "application/x-tex";
            extensionToMime[".tif"] = "image/tiff";
            extensionToMime[".tiff"] = "image/tiff";
            extensionToMime[".tr"] = "application/x-troff";
            extensionToMime[".tsi"] = "audio/TSP-audio";
            extensionToMime[".tsp"] = "application/dsptype";
            extensionToMime[".tsv"] = "text/tab-separated-values";
            extensionToMime[".txt"] = "text/plain";
            extensionToMime[".unv"] = "application/i-deas";
            extensionToMime[".ustar"] = "application/x-ustar";
            extensionToMime[".vcd"] = "application/x-cdlink";
            extensionToMime[".vda"] = "application/vda";
            extensionToMime[".vrml"] = "model/vrml";
            extensionToMime[".wav"] = "audio/x-wav";
            extensionToMime[".wrl"] = "model/vrml";
            extensionToMime[".xbm"] = "image/x-xbitmap";
            extensionToMime[".xlc"] = "application/vnd.ms-excel";
            extensionToMime[".xll"] = "application/vnd.ms-excel";
            extensionToMime[".xlm"] = "application/vnd.ms-excel";
            extensionToMime[".xls"] = "application/vnd.ms-excel";
            extensionToMime[".xlw"] = "application/vnd.ms-excel";
            extensionToMime[".xml"] = "text/xml";
            extensionToMime[".xpm"] = "image/x-xpixmap";
            extensionToMime[".xwd"] = "image/x-xwindowdump";
            extensionToMime[".xyz"] = "chemical/x-pdb";
            extensionToMime[".zip"] = "application/zip";
            extensionToMime[".m4v"] = "video/x-m4v";
            extensionToMime[".webm"] = "video/webm";
            extensionToMime[".ogv"] = "video/ogv";
            extensionToMime[".xap"] = "application/x-silverlight-app";
            extensionToMime[".mp4"] = "video/mp4";
            extensionToMime[".wmv"] = "video/x-ms-wmv";

        }

        /// <summary>
        /// URL encodes a string. If the path property is specified,
        /// the accepted path characters {/+:} are not encoded.
        /// </summary>
        /// <param name="data">The string to encode</param>
        /// <param name="path">Whether the string is a URL path or not</param>
        /// <returns></returns>
        public static string UrlEncode(string data, bool path)
        {
            return AWSSDKUtils.UrlEncode(data, path);
        }

        /// <summary>
        /// Converts a non-seekable stream into a System.IO.MemoryStream.
        /// A MemoryStream's position can be moved arbitrarily
        /// </summary>
        /// <param name="input">The stream to be converted</param>
        /// <returns>A seekable MemoryStream</returns>
        /// <remarks>MemoryStreams use byte arrays as their backing store.
        /// Please use this judicially as it is likely that a very large
        /// stream will cause system resources to be used up.
        /// </remarks>
        public static System.IO.Stream MakeStreamSeekable(System.IO.Stream input)
        {
            System.IO.MemoryStream output = new System.IO.MemoryStream();
            const int readSize = 32 * 1024;
            byte[] buffer = new byte[readSize];

            int count = 0;
            using (input)
            {
                while ((count = input.Read(buffer, 0, readSize)) > 0)
                {
                    output.Write(buffer, 0, count);
                }
            }

            output.Position = 0;
            return output;
        }

        /// <summary>
        /// Formats the current date as a GMT timestamp
        /// </summary>
        /// <returns>A GMT formatted string representation
        /// of the current date and time
        /// </returns>
        public static string FormattedCurrentTimestamp
        {
            get
            {
                return AWSSDKUtils.FormattedCurrentTimestampGMT;
            }
        }

        /// <summary>
        /// Computes RFC 2104-compliant HMAC signature
        /// </summary>
        /// <param name="data">The data to be signed</param>
        /// <param name="key">The secret signing key</param>
        /// <param name="algorithm">The algorithm to sign the data with</param>
        /// <returns>A string representing the HMAC signature</returns>
        public static string Sign(string data, string key, KeyedHashAlgorithm algorithm)
        {
            if (key == null)
            {
                throw new Exception("The AWS Secret Access Key specified is NULL!");
            }

            return AWSSDKUtils.HMACSign(data, key, algorithm);
        }

        /// <summary>
        /// Version2 S3 buckets adhere to RFC 1035:
        /// <list type="number">
        /// <item>Less than 255 characters, with each label less than 63 characters.</item>
        /// <item>Label must start with a letter</item>
        /// <item>Label must end with a letter or digit</item>
        /// <item>Label can have a string of letter, digits and hyphens in the middle.</item>
        /// <item>Although names can be case-sensitive, no significance is attached to the case.</item>
        /// <item>RFC 1123: Allow label to start with letter or digit (e.g. 3ware.com works)</item>
        /// <item>RFC 2181: No restrictions apart from the length restrictions.</item>
        /// </list>
        /// S3 V2 will start with RFCs 1035 and 1123 and impose the following additional restrictions:
        /// <list type="number">
        /// <item>Length between 3 and 63 characters (to allow headroom for upper-level domains,
        ///     as well as to avoid separate length restrictions for bucket-name and its labels</item>
        /// <item>Only lower-case to avoid user confusion.</item>
        /// <item>No dotted-decimal IPv4-like strings</item>
        /// </list>
        /// </summary>
        /// <param name="bucketName">The BucketName to validate if V2 addressing should be used</param>
        /// <returns>True if the BucketName should use V2 bucket addressing, false otherwise</returns>
        /// <seealso href="http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html">
        /// S3 v2 Bucket restrictions</seealso>
        public static bool ValidateV2Bucket(string bucketName)
        {
            if (String.IsNullOrEmpty(bucketName))
            {
                throw new ArgumentNullException("bucketName", "Please specify a bucket name");
            }

            if (bucketName.StartsWith("s3.amazonaws.com"))
            {
                return false;
            }

            // If the entire S3 URL is passed instead of just the bucketName, 
            // strip out the Amazon S3 part of the URL
            int idx = bucketName.IndexOf(".s3.amazonaws.com");
            if (idx > 0)
            {
                bucketName = bucketName.Substring(0, idx);
            }

            if (bucketName.Length < S3Constants.MinBucketLength ||
                 bucketName.Length > S3Constants.MaxBucketLength ||
                 bucketName.StartsWith(".") ||
                 bucketName.EndsWith("."))
            {
                return false;
            }

            // Check not IPv4-like
            Regex ipv4 = new Regex("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$");
            if (ipv4.IsMatch(bucketName))
            {
                return false;
            }

            // Check each label
            Regex v2Regex = new Regex("^[a-z0-9]([a-z0-9\\-]*[a-z0-9])?$");
            string[] labels = bucketName.Split("\\.".ToCharArray());
            foreach (string label in labels)
            {
                if (!v2Regex.IsMatch(label))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Determines MIME type from a file extension
        /// </summary>
        /// <param name="ext">The extension of the file</param>
        /// <returns>The MIME type for the extension, or text/plain</returns>
        public static string MimeTypeFromExtension(string ext)
        {
            if (extensionToMime.ContainsKey(ext))
            {
                return extensionToMime[ext];
            }
            else
            {
                return "application/octet-stream";
            }
        }

    }
}
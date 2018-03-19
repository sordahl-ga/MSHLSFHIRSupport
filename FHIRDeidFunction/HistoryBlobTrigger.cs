using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using Hl7.Fhir.Serialization;
using Hl7.Fhir.Model;
namespace FHIRDeidFunction
{
    public static class HistoryBlobTrigger
    {
        [FunctionName("HistoryBlobTrigger")]
        public static void Run([BlobTrigger("fhirhistory/Patient/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, TraceWriter log)
        {
            StreamReader sr = new StreamReader(myBlob);
            string raw = sr.ReadToEnd();
            var parsersettings = new ParserSettings();
            parsersettings.AcceptUnknownMembers = true;
            parsersettings.AllowUnrecognizedEnums = true;
            var parser = new FhirJsonParser(parsersettings);
            var reader = FhirJsonParser.CreateFhirReader(raw);
            var pat = parser.Parse<Patient>(reader);
            /*********************************De-Identify Safe Harbor*****************************************************/
            //HTML
            pat.Text = null;
            //Ids
            pat.Identifier = null;
            //BirthDate
            if (pat.BirthDate != null && pat.BirthDate.Length > 4)
            {
                pat.BirthDateElement = new Date(Convert.ToInt32(pat.BirthDate.Substring(0, 4)));
            }
            //Names
            pat.Name = new List<HumanName>();
            //Addresses
            var addrs = pat.Address;
            if (addrs != null)
              {
                    foreach (Address a in addrs)
                    {
                        a.Text = null;
                        if (a.PostalCode != null && a.PostalCode.Length > 3)
                        {
                            var opc = a.PostalCode;
                            a.PostalCodeElement = null;
                            a.PostalCode = opc.Substring(0, 3);
                    }
                        a.Line = null;
                        a.LineElement = null;
                        a.City = null;

                    }
                }
                //Telecom
                pat.Telecom = null;
                //Contact
                pat.Contact = null;
                //Contained Resources
                pat.Contained = null;
                //Photo
                pat.Photo = null;
                string didpat = FhirSerializer.SerializeToJson(pat);
            /*************************************Store Deidentified Safe Harbor Patient*********************************/
            var storage = Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storage);
            // Create the table if it doesn't exist.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            var blob = blobClient.GetContainerReference("fhirdeidentified");
            blob.CreateIfNotExists();
            try
            {
                var resource = System.Text.Encoding.UTF8.GetBytes(didpat);
                CloudBlockBlob blockBlob = blob.GetBlockBlobReference("Patient/" + pat.Id + "/" + pat.Meta.VersionId);
                using (var stream = new MemoryStream(resource, writable: false))
                {
                    blockBlob.UploadFromStream(stream);
                }
                
            }
            catch (Exception e)
            {
                log.Error("Error inserting deid {0}", e);
                
            }
            log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes.  Contents: {didpat}");
        }
    }
}

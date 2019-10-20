using Google.Apis.Auth.OAuth2; // Google.Apis.Auth --version 1.30.0
using Google.Cloud.Storage.V1; // Google.Cloud.Storage.V1
using Google.Cloud.PubSub.V1; // Google.Cloud.PubSub.V1
using System;
using System.IO;
using System.Runtime.InteropServices;
using static System.Console;
using System.Text;
using System.Linq;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace ConsoleGoogleBucket
{
    public static class Program
    {

        //File Paths
        static string _message = "";
        static string _bucketName = "mysubpub_bucket_files";
        static string _credentialsPath = "C:\\Test\\GoogleBucket\\MySubPub-e47d82d44e22.json";
        static string _folderPathUpload = "C:\\Test\\GoogleBucket\\UL";
        static string _folderPathDownload = "C:\\Test\\GoogleBucket\\DL\\";

        //Bucket
        private static StorageClient _storageClient;

        //PUB SUB
        private static TopicName _topicName;
        private static Options _options;
        private static readonly string _projectId = "mysubpub";

        public static void Main(string[] args)
        {

            //BUCKET:
            // Explicitly use service account credentials by specifying the private key file.
            // The service account should have Object Manage permissions for the bucket.
            //*******This does not require the SKD*****************************
            GoogleCredential credential = null;
            using (var jsonStream = new FileStream(_credentialsPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                credential = GoogleCredential.FromStream(jsonStream);
            }

            _storageClient = StorageClient.Create(credential);


            //PUB SUB

            _options = new Options();
            _topicName = new TopicName(_projectId, _options.TopicId);


            UploadAndPubLish();
            Console.WriteLine("UPLOAD Finished: Press any key to continue.");
            Console.ReadKey();

            SubscribAndDownload(true);
            WriteLine();
            Console.WriteLine("DownLoad Finished: Press any key to continue.");
            Console.ReadKey();




            // List objects
            WriteLine();
            WriteLine();
            WriteLine("IN CLOUD:");
            foreach (var obj in _storageClient.ListObjects(_bucketName, ""))
            {
                Console.WriteLine(obj.Name);
            }
            WriteLine();

            WriteLine("IN C: Dirve:");
            foreach (var obj in Directory.GetFiles(_folderPathDownload))
            {
                Console.WriteLine(obj);
            }
            WriteLine();

            WriteLine("**Environment**");
            WriteLine($"Platform: .NET Core 2.0");
            // WriteLine($"OS: {RuntimeInformation.OSDescription}");
            WriteLine(_message);
            WriteLine();
            Console.WriteLine("Press any key to close");
            Console.ReadKey();
        }

        //UPload All 
        private static void UploadAndPubLish()
        {
            foreach (string file in Directory.EnumerateFiles(_folderPathUpload, "*.json"))
            {
                string fileName = Path.GetFileName(file);
                try
                {
                    using (var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        _storageClient.UploadObject(_bucketName, fileName, "application/json", fileStream);
                    }

                    Task.Factory.StartNew((Action)(() => PubLishFile(fileName)));
                    //PubLishFile(fileName);
                }
                catch (Exception e)
                {
                    _message = e.Message;
                }

            }
        }

        private static void PubLishFile(string fileName)
        {
            PublisherServiceApiClient publishClient = PublisherServiceApiClient.Create();

            var message = new QueueMessage() { FileName = fileName };
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(message);

            publishClient.Publish(_topicName, new[] { new PubsubMessage()
            {
                Data = Google.Protobuf.ByteString.CopyFromUtf8(json)
            } });
        }


        public static void SubscribAndDownload(bool acknowledge)
        {
            // [START pubsub_subscriber_sync_pull]

            SubscriptionName subscriptionName = new SubscriptionName(_projectId, _options.SubscriptionId);

            SubscriberServiceApiClient subscriberClient = SubscriberServiceApiClient.Create();

            // Pull messages from server,
            // allowing an immediate response if there are no messages.
            PullResponse response = subscriberClient.Pull(subscriptionName, returnImmediately: true, maxMessages: 20);
            // Print out each received message.

            foreach (ReceivedMessage message in response.ReceivedMessages)
            {
                string text = Encoding.UTF8.GetString(message.Message.Data.ToArray());
                Console.WriteLine($"Message {message.Message.MessageId}: {text}");
                try
                {
                    // Unpack the message.
                    byte[] json = message.Message.Data.ToByteArray();
                    var qmessage = JsonConvert.DeserializeObject<QueueMessage>(Encoding.UTF8.GetString(json));

                    //Task.Factory.StartNew((Action)(() => Download(qmessage.FileName)));
                    Download(qmessage.FileName);

                    // If acknowledgement required, send to server.
                    if (acknowledge)
                    {
                        Task.Factory.StartNew((Action)(() => AcknowledgeMessage(subscriberClient, subscriptionName, response)));
                        //AcknowledgeMessage(subscriberClient, subscriptionName, response)

                    }
                    // [END pubsub_subscriber_sync_pull]

                }
                catch (Exception e)
                {
                    _message = e.Message;
                }
            }
        }


        private static void Download(string fileName)
        {
            var file = _folderPathDownload + fileName;

            using (var fileStream = File.Create(file))
            {
                _storageClient.DownloadObject(_bucketName, fileName, fileStream);
            }
        }

        private static void AcknowledgeMessage(SubscriberServiceApiClient subscriberClient, SubscriptionName subscriptionName, PullResponse response)
        {
            subscriberClient.Acknowledge(subscriptionName, response.ReceivedMessages.Select(msg => msg.AckId));
        }




        private class QueueMessage
        {
            public string FileName;
        };

        public class Options
        {
            public string TopicId = "file-process-queue";
            public string SubscriptionId = "shared-file-subscription";
        };


        //Download All
        private static void DownloadAll()
        {
            foreach (var obj in _storageClient.ListObjects(_bucketName, ""))
            {
                var file = _folderPathDownload + obj.Name;

                using (var fileStream = File.Create(file))
                {
                    _storageClient.DownloadObject(_bucketName, obj.Name, fileStream);
                }
            }
        }

    }
}

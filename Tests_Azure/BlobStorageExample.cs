using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;


namespace Tests_Azure
{
    public class BlobStorageExample
    {
        const string SampleFilePath = @"E:\Idealine\Quadient\test_PDF\10.1mb.pdf";

        const string ConnectionString =
            @"DefaultEndpointsProtocol=https;AccountName=testidealine;AccountKey=ZaHYr5EFw4QFT1LukCqkutKy33mWsEFqoecdU+SnZyvX7KZYeMx8xOmDi1uAfTGYySKXcyYofAq1VQlZsjZYLw==;EndpointSuffix=core.windows.net";

        const string ContainerName = "test1";
        const string BlobName = "testing_1.bin";

        public void Init()
        {
            // create container at Azure
            var container = new BlobContainerClient(ConnectionString, ContainerName);
            container.DeleteBlobIfExists(BlobName);
        }

        public async Task UploadWithReadingAsync(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFilePath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            try
            {
                await using var localFileStream = File.OpenRead(SampleFilePath);
                var blockBlobClient = BlockBlobClient(container);

                await using var stream = await blockBlobClient.OpenWriteAsync(true);
                await localFileStream.CopyToAsync(stream);

                await GetBlobsByHierarchyAsPagesAsync(container);

                await localFileStream.FlushAsync();
                await stream.FlushAsync();

                Console.WriteLine("------------------");
                Console.WriteLine("Reading after upload first thread");
                Console.WriteLine("------------------");

                await GetBlobsByHierarchyAsPagesAsync(container);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Upload exception - {ex}");
            }
        }

        public async Task UploadAsync(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFilePath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            try
            {
                await using var localFileStream = File.OpenRead(SampleFilePath);
                var blockBlobClient = BlockBlobClient(container);

                await using var stream = await blockBlobClient.OpenWriteAsync(true);
                await localFileStream.CopyToAsync(stream);

                await localFileStream.FlushAsync();
                await stream.FlushAsync();

                Console.WriteLine("------------------");
                Console.WriteLine("Reading after upload");
                Console.WriteLine("------------------");

                await GetBlobsByHierarchyAsPagesAsync(container);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Upload exception - {ex}");
            }
        }

        public async Task UploadAfterFullUploadAsync(BlobContainerClient container)
        {
            await UploadAsync(container);

            Console.WriteLine("Press Enter to exit");
            Console.ReadLine();
        }

        public async Task<IAsyncEnumerable<Page<BlobHierarchyItem>>> GetBlobsByHierarchyAsPagesAsync(
            BlobContainerClient blobContainerClient)
        {
            var resultSegment = blobContainerClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.Uncommitted)
                .AsPages(default, 50);

            await foreach (Azure.Page<BlobHierarchyItem> blobPage in resultSegment)
            {
                foreach (BlobHierarchyItem blobHierarchyItem in blobPage.Values)
                {
                    Console.WriteLine("Blob.Name " + blobHierarchyItem.Blob.Name);
                    Console.WriteLine(
                        "Blob.Properties.ContentLength " + blobHierarchyItem.Blob.Properties.ContentLength);
                    Console.WriteLine("Blob.Properties.ETag " + blobHierarchyItem.Blob.Properties.ETag);
                    Console.WriteLine("--------------");
                }
            }

            return resultSegment;
        }

        public async Task ContinueWhenFullUpload(BlobContainerClient container)
        {
            do
            {
                Console.WriteLine($"Waiting for fully uploaded.");
                Task.Delay(1000).Wait();
            } while (!await IsFullUploadedByContentLength(container));
        }

        public async Task<bool> IsFullUploadedByContentLength(BlobContainerClient blobContainerClient)
        {
            var resultSegment = await GetBlobsByHierarchyAsPagesAsync(blobContainerClient);

            await foreach (Azure.Page<BlobHierarchyItem> blobPage in resultSegment)
            {
                if (blobPage.Values.Any(blobHierarchyItem => blobHierarchyItem.Blob.Properties.ContentLength > 0))
                {
                    Console.WriteLine($"Blob is fully uploaded.");
                    return true;
                }
            }

            return false;
        }

        public async Task GetBlobsByHierarchyAsPagesWithDelayAsync(BlobContainerClient blobContainerClient, int delay)
        {
            Task.Delay(delay).Wait();
            await GetBlobsByHierarchyAsPagesAsync(blobContainerClient);
        }

        public BlockBlobClient BlockBlobClient(BlobContainerClient container)
        {
            var blockBlobClient = container.GetBlockBlobClient(BlobName);
            return blockBlobClient;
        }


        public BlobContainerClient GetBlobContainerClient()
        {
            var container = new BlobContainerClient(ConnectionString, ContainerName);
            return container;
        }


        // public async Task ReadAsync()
        // {
        //     //Task.Delay(5000).Wait();
        //
        //     var container = new BlobContainerClient(ConnectionString, ContainerName);
        //
        //     try
        //     {
        //         // Get a reference to a blob
        //         var blob = container.GetBlobClient(BlobName);
        //
        //         var bufSize = 2 * 1024 * 1024;
        //         Console.WriteLine("Read - before open for read");
        //
        //         var openedStream = await blob.OpenReadAsync(new BlobOpenReadOptions(false) { BufferSize = bufSize });
        //         Console.WriteLine("Read - after open for read");
        //
        //         var fileStream = File.OpenWrite(SampleFilePathFromBlob);
        //         openedStream.CopyTo(fileStream);
        //         openedStream.Flush();
        //         Console.WriteLine("Read - Read finished");
        //
        //         var length = new FileInfo(SampleFilePathFromBlob).Length;
        //         Console.WriteLine($"Read - read file size: {length}");
        //     }
        //     catch (Exception ex)
        //     {
        //         Console.WriteLine($"Read exception - {ex}");
        //     }
        //     finally
        //     {
        //         // Clean up after the test when we're finished
        //         //await container.DeleteAsync();
        //     }
        // }

        // public async Task ReadWithWaitForFullyUploadedAsync(int maxSeconds)
        // {
        //     string connectionString = ConnectionString;
        //     BlobContainerClient container = new BlobContainerClient(connectionString, ContainerName);
        //
        //     try
        //     {
        //         // Get a reference to a blob
        //         BlobClient blob = container.GetBlobClient(BlobName);
        //         bool isFullUploaded = false;
        //         do
        //         {
        //             maxSeconds--;
        //             isFullUploaded = IsFullUploaded(blob);
        //             if (maxSeconds <= 0)
        //             {
        //                 throw new Exception("Waiting for uploaded blob failed");
        //             }
        //
        //             Console.WriteLine($"Waiting for fully uploaded {maxSeconds} s.");
        //             Task.Delay(1000).Wait();
        //         } while (!isFullUploaded);
        //
        //         int bufSize = 2 * 1024 * 1024;
        //         Console.WriteLine("ReadWithWaitForFullyUploadedAsync - before open for read");
        //         var openedStream = await blob.OpenReadAsync(new BlobOpenReadOptions(false) { BufferSize = bufSize });
        //         Console.WriteLine("ReadWithWaitForFullyUploadedAsync - after open for read");
        //         FileStream fileStream = File.OpenWrite(SampleFilePathFromBlob2);
        //         openedStream.CopyTo(fileStream);
        //         openedStream.Flush();
        //         openedStream.Close();
        //         Console.WriteLine("ReadWithWaitForFullyUploadedAsync - Read finished");
        //
        //         long length = new FileInfo(SampleFilePathFromBlob2).Length;
        //         Console.WriteLine($"ReadWithWaitForFullyUploadedAsync - read file size: {length}");
        //     }
        //     catch (Exception ex)
        //     {
        //         Console.WriteLine($"ReadWithWaitForFullyUploadedAsync exception - {ex}");
        //     }
        //     finally
        //     {
        //         // Clean up after the test when we're finished
        //         //await container.DeleteAsync();
        //     }
        // }

        // public async Task PossibleWriteErrorWithNoFlush()
        // {
        //     string connectionString = ConnectionString;
        //     BlobContainerClient container = new BlobContainerClient(connectionString, ContainerName);
        //
        //     try
        //     {
        //         // Get a reference to a blob
        //         BlockBlobClient blockBlobClient = container.GetBlockBlobClient(BlobName);
        //         await using var blobStream = await blockBlobClient.OpenWriteAsync(true);
        //         await using var localFileStream = File.OpenRead(SampleFilePath);
        //         Console.WriteLine("PossibleWriteErrorWithNoFlush - after OpenWriteAsync");
        //         await localFileStream.CopyToAsync(blobStream);
        //         Console.WriteLine("PossibleWriteErrorWithNoFlush - after CopyToAsync");
        //     }
        //     catch (Exception ex)
        //     {
        //     }
        // }
    }
}
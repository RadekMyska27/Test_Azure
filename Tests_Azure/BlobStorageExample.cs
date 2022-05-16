using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;


namespace Tests_Azure
{
    public class BlobStorageExample
    {
        const string SampleFile10MbPath = @"E:\Idealine\Quadient\test_PDF\Test_txt.txt";
        const string SampleFilePath2 = @"E:\Idealine\Quadient\test_PDF\10.1_2mb.pdf";

        const string ConnectionString =
            @"DefaultEndpointsProtocol=https;AccountName=testidealine;AccountKey=ZaHYr5EFw4QFT1LukCqkutKy33mWsEFqoecdU+SnZyvX7KZYeMx8xOmDi1uAfTGYySKXcyYofAq1VQlZsjZYLw==;EndpointSuffix=core.windows.net";

        const string ContainerName = "test1";
        const string BlobName = "testing_1.bin";

        public async Task Init()
        {
            // create container at Azure
            var container = new BlobContainerClient(ConnectionString, ContainerName);
            await container.DeleteBlobIfExistsAsync(BlobName, DeleteSnapshotsOption.IncludeSnapshots);
        }

        public async Task UploadWithReadingAsync(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFile10MbPath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            try
            {
                await using var localFileStream = File.OpenRead(SampleFile10MbPath);
                var blockBlobClient = BlockBlobClient(container);

                await using var blob = await blockBlobClient.OpenWriteAsync(true);
                await localFileStream.CopyToAsync(blob);
                
                await GetBlobsByHierarchyAsPagesAsync(container);

                await localFileStream.FlushAsync();
                await blob.FlushAsync();

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
            var sampleFile = new FileInfo(SampleFile10MbPath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");
           
            await BlobExistsAsync(container);

            try
            {
                await using var localFileStream = File.OpenRead(SampleFile10MbPath);
                var blockBlobClient = BlockBlobClient(container);

                await using var blob = await blockBlobClient.OpenWriteAsync(true);
                await localFileStream.CopyToAsync(blob);

                // await localFileStream.FlushAsync();
                // await blob.FlushAsync();

                Console.WriteLine("------------------");
                Console.WriteLine("Reading after upload");
                Console.WriteLine("------------------");

                // await GetBlobsByHierarchyAsPagesAsync(container);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Upload exception - {ex}");
            }
        }

        public async Task Upload(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFilePath2);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            try
            {
                await using var localFileStream = File.OpenRead(SampleFilePath2);
                var textReader = new StreamReader(localFileStream);

                // await using var fileStram2 = File.OpenWrite(SampleFilePath2);
                
                var blockBlobClient = BlockBlobClient(container);

                await using var blob = await blockBlobClient.OpenWriteAsync(true);

                var streamWriter = new StreamWriter(blob, Encoding.UTF8);

                await streamWriter.WriteAsync(await textReader.ReadToEndAsync());

                await streamWriter.FlushAsync();
                streamWriter.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        } 

        public async Task UploadWithGetBlobsAsyncReadingAsync(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFile10MbPath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            try
            {
                await using var localFileStream = File.OpenRead(SampleFile10MbPath);
                var blockBlobClient = BlockBlobClient(container);

                await using var blob = await blockBlobClient.OpenWriteAsync(true);
                await localFileStream.CopyToAsync(blob);

                await GetBlobsAsync(container);

                await localFileStream.FlushAsync();
                await blob.FlushAsync();

                Console.WriteLine("------------------");
                Console.WriteLine("Reading after upload first thread");
                Console.WriteLine("------------------");

                await GetBlobsAsync(container);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Upload exception - {ex}");
            }
        }
        
        public async Task UploadTenThousandTimeAsync(BlobContainerClient container)
        {
            var sampleFile = new FileInfo(SampleFile10MbPath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");

            for (var i = 0; i < 9999; i++)
            {
                Console.WriteLine("cycle " + i);
                
                try
                {
                    await using var currentStream = File.OpenRead(SampleFile10MbPath);

                    await using var blob = await CreateBlobAsync(container);
                    await currentStream.CopyToAsync(blob);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Upload exception - {ex}");
                }
            }
        }
        
        // await using var currentStream = await _blobStorage.GetBlobAsync(docLocation);
       
        // await using var newStream = await _converterBlobStorage.CreateBlobAsync(docLocation);
        // await currentStream.CopyToAsync(newStream, cancellationToken);
        
        public async Task UploadTenThousandTimeWithGuidAsync(BlobContainerClient container, Guid guid)
        {
            var sampleFile = new FileInfo(SampleFile10MbPath);
            Console.WriteLine($"Upload - local file {sampleFile.Name} size for upload: {sampleFile.Length}");
            

            for (var i = 0; i < 9999; i++)
            {
                Console.WriteLine("cycle " + i);

                try
                {
                    await using var currentStream = File.OpenRead(SampleFile10MbPath);

                    await using var blob = await CreateBlobWithGuidAsync(container, guid);
                    await currentStream.CopyToAsync(blob);

                    await GetBlobsByHierarchyAsPagesAsync(container);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Upload exception - {ex}");
                }
            }
        }

        public async Task<Stream> CreateBlobAsync(BlobContainerClient container)
        {
            try
            {
                var blob = GetCloudBlobAsync(container);
                return await blob.OpenWriteAsync(true);
            }
            catch (Exception e)
            {
                throw;
            }
        }
        
        public async Task<Stream> CreateBlobWithGuidAsync(BlobContainerClient container, Guid guid)
        {
            try
            {
                var blob = GetCloudBlobsWithGuidAsync(container, guid);
                return await blob.OpenWriteAsync(true);
            }
            catch (Exception e)
            {
                throw;
            }
        }
        
        BlockBlobClient GetCloudBlobAsync(BlobContainerClient container)
        {
            var blob = container.GetBlockBlobClient(BlobName);
            return blob;
        }
        
        BlockBlobClient GetCloudBlobsWithGuidAsync(BlobContainerClient container, Guid guid)
        {
            var blob = container.GetBlockBlobClient(guid.ToString());
            
            return blob;
        }

        public async Task UploadAfterFullUploadAsync(BlobContainerClient container)
        {
            await UploadAsync(container);

            Console.WriteLine("Press Enter to exit");
            Console.ReadLine();
        }

        public async Task<AsyncPageable<BlobItem>> GetBlobsAsync(BlobContainerClient blobContainerClient)
        {
            var resultSegment = blobContainerClient.GetBlobsAsync(BlobTraits.None, BlobStates.Uncommitted);

            await foreach (var blobItem in resultSegment)
            {
                Console.WriteLine("blobItem.Name " + blobItem.Name);
                Console.WriteLine(
                    "blobItem.Properties.ContentLength " + blobItem.Properties.ContentLength);
                Console.WriteLine("blobItem.Properties.ETag " + blobItem.Properties.ETag);
                Console.WriteLine("--------------");
            }

            return resultSegment;
        }


        public async Task<IAsyncEnumerable<Page<BlobHierarchyItem>>> GetBlobsByHierarchyAsPagesAsync(
            BlobContainerClient blobContainerClient)
        {
            var resultSegment = blobContainerClient.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None)
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
            } while (!await IsFullUploadedByHierarchyAndContentLength(container));
        }

        public async Task ContinueWhenFullUploadWithGetBlobsAsync(BlobContainerClient container)
        {
            do
            {
                Console.WriteLine($"Waiting for fully uploaded.");
                Task.Delay(1000).Wait();
            } while (!await IsFullUploadedGetBlobsAsyncAndContentLength(container));
        }
        
        public async Task ContinueWhenFullUploadWithBlobExistsAsync(BlobContainerClient container)
        {
            do
            {
                Console.WriteLine($"Waiting for fully uploaded.");
                Task.Delay(1000).Wait();
            } while (!await BlobExistsAsync(container));
        }

        public async Task<bool> IsFullUploadedByHierarchyAndContentLength(BlobContainerClient blobContainerClient)
        {
            var resultSegment = await GetBlobsByHierarchyAsPagesAsync(blobContainerClient);

            await foreach (Azure.Page<BlobHierarchyItem> blobPage in resultSegment)
            {
                if (blobPage.Values.Any(blobHierarchyItem => blobHierarchyItem.Blob.Properties.ContentLength > 0 &&
                                                             blobHierarchyItem.Blob.Name == BlobName))
                {
                    Console.WriteLine($"Blob is fully uploaded.");
                    return true;
                }
            }

            return false;
        }

        public async Task<bool> IsFullUploadedGetBlobsAsyncAndContentLength(BlobContainerClient blobContainerClient)
        {
            var resultSegment = await GetBlobsAsync(blobContainerClient);

            await foreach (var blobItem in resultSegment)
            {
                if (blobItem.Name == BlobName && blobItem.Properties.ContentLength > 0)
                {
                    Console.WriteLine($"Blob is fully uploaded.");
                    return true;
                }
            }

            return false;
        }

        public async Task<bool> IsBlobExistInContainer(BlobContainerClient blobContainerClient)
        {
            if (!await blobContainerClient.ExistsAsync())
                return false;

            await foreach (var blobItem in blobContainerClient.GetBlobsAsync().AsPages(default, 50))
            {
                if (blobItem.Values.Any(item => item.Name == BlobName))
                {
                    return true;
                }
            }

            return false;
        }

        public async Task GetBlobsByHierarchyAsPagesWithDelayAsync(BlobContainerClient blobContainerClient, int delay)
        {
            Task.Delay(delay).Wait();
            await BlobExistsAsync(blobContainerClient);
            await GetBlobsByHierarchyAsPagesAsync(blobContainerClient);
        }

        public async Task GetBlobsWithDelayAsync(BlobContainerClient blobContainerClient, int delay)
        {
            Task.Delay(delay).Wait();
            await GetBlobsAsync(blobContainerClient);
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

        public BlobContainerClient GetBlobServiceClient()
        {
            return new BlobServiceClient(ConnectionString).GetBlobContainerClient(ContainerName);
        }
        
        public async Task<bool> BlobExistsAsync(BlobContainerClient container)
        {
            try
            {
                var blobReference = container.GetBlobClient(BlobName);
                var existsAsync = await blobReference.ExistsAsync();
                
                Console.WriteLine(existsAsync ? "Blob with name {0} exist" : "Blob with name {0} NOT exist", BlobName);

                return existsAsync;
            }
            catch (Exception e)
            {
                throw new Exception();
            }
        }

        [Flags]
        public enum TestFlags
        {
            NormalUser = 1,
            Custodian = 2,
            Finance = 4,
            SuperUser = Custodian | Finance,
            All = Custodian | Finance | NormalUser
        }

        public class TestFlag
        {
            public TestFlags Flags { get; set; }
        }

        public List<TestFlag> BlobFlags = new List<TestFlag>
        {
            new TestFlag { Flags = TestFlags.All },
            new TestFlag { Flags = TestFlags.NormalUser },
            new TestFlag { Flags = TestFlags.Custodian },
            new TestFlag { Flags = TestFlags.Finance },
        };

        public List<TestFlag> GetTestsFlags(TestFlags tags)
        {
            return this.BlobFlags.Where(item => item.Flags == tags).ToList();
        }

        public async Task IsOverTime()
        {
            var start = DateTime.UtcNow.AddMinutes(1);

            while (start > DateTime.UtcNow)
            {
                Console.WriteLine("jeste ne");
                Task.Delay(1000).Wait();
            }
            Console.WriteLine("Uz je to tu");
            Console.ReadLine();
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
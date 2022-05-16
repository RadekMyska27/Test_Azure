// See https://aka.ms/new-console-template for more information

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Tests_Azure;

var blobStorageExample = new BlobStorageExample();
await blobStorageExample.Init();

// scenario 1 - one thread only upload with GetBlobsByHierarchyAsPagesAsync
// OneThreadReadDuringAndAfterUploadScenario();

// scenario 2 - two thread upload and reading after Delay with GetBlobsByHierarchyAsPagesAsync
// TwoThreadUploadReadingWithDelayDuringUploadScenario();

// scenario 3 - simulate exception - condition not met with GetBlobsByHierarchyAsPagesAsync
// SimulateExceptionConditionNotMetScenario();

// scenario 4 - waiting for full upload by checking content length of the uploaded blob with GetBlobsByHierarchyAsPagesAsync
// WaitUntilBlobHasSetContentLength();

// scenario 5 - for reading list use method GetBlobsAsync
// OneThreadGetBlobsAsyncReadDuringAndAfterUploadScenario();

// scenario 6 - two thread upload and reading after Delay
// TwoThreadUploadReadingGetBlobsAsyncWithDelayDuringUploadScenario();

// scenario 7 - wait for full upload with GetBlobsAsync
// WaitUntilBlobHasSetContentLengthWithGetBlobsAsync();

// scenario 8 - wait for full upload with GetBlobsAsync NOT FIXED !!!
// WaitUntilBlobHasSetContentLengthWithBlobExistsAsync();

// scenario 9 flags
// await TestMethod();

// blobStorageExample.IsOverTime();

// scenario 10 call blob write 10 000 times
// await CallUploadTenThousandTimeAsync();

// scenario 11 call blob write 10 000 times with 10 blobs in package
CallUploadTenThousandTimeWithGuidAsync();
// DeleteContainer();

void OneThreadReadDuringAndAfterUploadScenario()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadWithReadingAsync(container);
    tasks.Add(upload1);

    ResolveScenario(tasks);
}

void TwoThreadUploadReadingWithDelayDuringUploadScenario()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };
    const int delay = 1000;

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    Console.WriteLine("Reading from second thread");
    var upload2 = blobStorageExample.GetBlobsByHierarchyAsPagesWithDelayAsync(container, delay);
    tasks.Add(upload2);

    ResolveScenario(tasks);
}

async Task TestMethod()
{
    var container = blobStorageExample.GetBlobContainerClient();

    // await blobStorageExample.UploadAsync(container);
    //

    await blobStorageExample.Upload(container);
    await blobStorageExample.GetBlobsByHierarchyAsPagesAsync(container);

    Console.WriteLine("Press Enter to exit");
    Console.ReadLine();
}

void SimulateExceptionConditionNotMetScenario()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    Task.Delay(100).Wait();
    var upload2 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload2);

    ResolveScenario(tasks);
}

void WaitUntilBlobHasSetContentLength()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    var readFullyUploadTask = blobStorageExample.ContinueWhenFullUpload(container).ContinueWith(async _ =>
    {
        await blobStorageExample.UploadAfterFullUploadAsync(container);
    });
    tasks.Add(readFullyUploadTask);

    Task.WaitAll(tasks.ToArray());
    Console.ReadLine();
}

void OneThreadGetBlobsAsyncReadDuringAndAfterUploadScenario()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadWithGetBlobsAsyncReadingAsync(container);
    tasks.Add(upload1);

    ResolveScenario(tasks);
}

void TwoThreadUploadReadingGetBlobsAsyncWithDelayDuringUploadScenario()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };
    const int delay = 1000;

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    Console.WriteLine("Reading from second thread");
    var upload2 = blobStorageExample.GetBlobsWithDelayAsync(container, delay);
    tasks.Add(upload2);

    ResolveScenario(tasks);
}

void WaitUntilBlobHasSetContentLengthWithGetBlobsAsync()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    var readFullyUploadTask = blobStorageExample.ContinueWhenFullUploadWithGetBlobsAsync(container)
        .ContinueWith(async _ => { await blobStorageExample.UploadAfterFullUploadAsync(container); });
    tasks.Add(readFullyUploadTask);

    Task.WaitAll(tasks.ToArray());
    Console.ReadLine();
}

void WaitUntilBlobHasSetContentLengthWithBlobExistsAsync()
{
    var container = blobStorageExample.GetBlobContainerClient();
    var tasks = new List<Task> { };

    var upload1 = blobStorageExample.UploadAsync(container);
    tasks.Add(upload1);

    var readFullyUploadTask = blobStorageExample.ContinueWhenFullUploadWithBlobExistsAsync(container)
        .ContinueWith(async _ => { await blobStorageExample.UploadAfterFullUploadAsync(container); });
    tasks.Add(readFullyUploadTask);

    Task.WaitAll(tasks.ToArray());
    Console.ReadLine();
}

void ResolveScenario(List<Task> tasks)
{
    Task.WaitAll(tasks.ToArray());

    Console.WriteLine("Press Enter to exit");
    Console.ReadLine();
}

void FlagsTest()
{
    Console.WriteLine("flags " + blobStorageExample.GetTestsFlags(BlobStorageExample.TestFlags.All).Count);

    Console.WriteLine("Press Enter to exit");
    Console.ReadLine();
}

async Task CallUploadTenThousandTimeAsync()
{
    var container = blobStorageExample.GetBlobContainerClient();
    await blobStorageExample.UploadTenThousandTimeAsync(container);
}

void CallUploadTenThousandTimeWithGuidAsync()
{
    var tasks = new List<Task> { };
    var container = blobStorageExample.GetBlobContainerClient();
    
    for (int i = 0; i < 9; i++)
    {
        var task1 = blobStorageExample.UploadTenThousandTimeWithGuidAsync(container, Guid.NewGuid());
        tasks.Add(task1);
    }

    ResolveScenario(tasks);
}

void DeleteContainer()
{
    var container = blobStorageExample.GetBlobContainerClient();
    container.Delete();
}

// check if blob is fuly uploaded 
// var readFullyUploadTask = blobStorageExample.ContinueWhenFullUpload(container).ContinueWith(_=>
// {
//     blobStorageExample.UploadSkymambaAsync(container);
// });
// tasks.Add(readFullyUploadTask);


// await blobStorageExample.GetBlobHierarchyItemAsync(container, BlobStates.Uncommitted);
// await blobStorageExample.GetBlobHierarchyItemAsync(container, BlobStates.None);


// await blobStorageExample.GetBlobHierarchyItemAsync(container, BlobStates.DeletedWithVersions);
// await blobStorageExample.GetBlobHierarchyItemAsync(container, BlobStates.Version);
// await blobStorageExample.GetBlobHierarchyItemAsync(container, BlobStates.Deleted);

// var readTask = blobStorageExample.ReadAsync();
// tasks.Add(readTask);

// var readFullyUploadTask = blobStorageExample.ReadWithWaitForFullyUploadedAsync(30).ContinueWith(_=>
// {
//     blobStorageExample.UploadSkymambaAsync();
// });
// tasks.Add(readFullyUploadTask);


// scenario 2 - try simulate error with switch read and write - same as in OMS500 code

// scenario 3
//await blobStorageExample.UploadSkymambaAsync();
//blobStorageExample.MatchPattern = "2022/2/8";
//await blobStorageExample.ListBlobPerDate(DateTime.MinValue, DateTime.MaxValue);

//var upload1 = blobStorageExample.PossibleWriteErrorWithNoFlush();
//upload1.ContinueWith(t => { var r = blobStorageExample.ReadAsync(); readTask.Add(r); });
//tasks.Add(upload1);
//Task.WaitAll(tasks.ToArray());
//Task.WaitAll(readTask.ToArray());
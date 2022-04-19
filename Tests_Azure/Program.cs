// See https://aka.ms/new-console-template for more information

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Tests_Azure;

var blobStorageExample = new BlobStorageExample();
blobStorageExample.Init();

// scenario 1 - one thread only upload
OneThreadReadDuringAndAfterUploadScenario();

// scenario 2 - two thread upload and reading after Delay
// TwoThreadUploadReadingWithDelayDuringUploadScenario();

// scenario 3 - simulate exception - condition not met
// SimulateExceptionConditionNotMetScenario();

// scenario 4 - waiting for full upload by checking content length of the uploaded blob
// WaitUntilBlobHasSetContentLength();

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

void ResolveScenario(List<Task> tasks)
{
    Task.WaitAll(tasks.ToArray());

    Console.WriteLine("Press Enter to exit");
    Console.ReadLine();
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
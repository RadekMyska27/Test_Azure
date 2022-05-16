using System.Collections.Generic;
using System.Threading;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace Tests_Azure
{
    public static class BlobContainerClientExtensions
    {
        public static IAsyncEnumerable<BlobHierarchyItem> GetItemsInDirectoryAsync(this BlobContainerClient client,
            BlobTraits traits = BlobTraits.None,
            BlobStates states = BlobStates.None,
            string? prefix = null,
            CancellationToken cancellationToken = default)
        {
            return client.GetBlobsByHierarchyAsync(traits, states);
        }
    }
}
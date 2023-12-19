using Meshmakers.Octo.ConstructionKit.Contracts;
using MongoDB.Bson;

namespace Meshmakers.Octo.Common.DistributionEventHub;

internal static class ObjectIdExtensions
{
    public static ObjectId ToObjectId(this OctoObjectId objectId)
    {
        return new ObjectId(objectId.ToString());
    }

    public static OctoObjectId ToOctoObjectId(this ObjectId objectId)
    {
        return new OctoObjectId(objectId.ToByteArray());
    }
}
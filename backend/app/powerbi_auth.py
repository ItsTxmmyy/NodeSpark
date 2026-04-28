
from fastapi import APIRouter
from motor.motor_asyncio import AsyncIOMotorClient

router = APIRouter(prefix="/powerbi", tags=["powerbi"])
client = AsyncIOMotorClient("mongodb://localhost:27017")
db = client["nodespark"]
collection = db["powerbi_tokens"]

@router.get("/{dataset_id}")
async def get_powerbi_dataset(dataset_id: str):
    # Fetch the dataset from MongoDB
    dataset_data = await collection.find_one({"dataset_id": dataset_id})
    if not dataset_data:
        return {"error": "Dataset not found"}
    
    # Return the dataset information
    return {
        "dataset_id": dataset_data["dataset_id"],
        "access_token": dataset_data["access_token"],
        "refresh_token": dataset_data["refresh_token"],
        "expires_at": dataset_data["expires_at"]
    }



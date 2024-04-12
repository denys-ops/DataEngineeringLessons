from fastapi import HTTPException, APIRouter
from starlette import status

from lesson_02.apis.schemas.models import TransformJobRequest
from lesson_02.apis.transform_api.service import convert_json_to_avro

transform_router = APIRouter()


@transform_router.post("/", status_code=status.HTTP_201_CREATED)
def transform_data(body: TransformJobRequest):
    """Transform required catalog to avro."""
    try:
        convert_json_to_avro(json_directory=body.raw_dir, avro_directory=body.stg_dir)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

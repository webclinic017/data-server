import json
import tempfile

from tqdm import tqdm

from .common.minio import exists_object, fget_object, client, remove_object


def clean(bucket_name):
    for obj in tqdm(list(client.list_objects(bucket_name, recursive=True))):
        object_name = obj.object_name
        if not exists_object(bucket_name, object_name):
            continue
        if not object_name.endswith(".json"):
            continue
        with tempfile.NamedTemporaryFile(suffix=".json") as temp_file:
            fget_object(bucket_name, object_name, temp_file.name)
            with open(temp_file.name, "r") as handler:
                response = json.load(handler)
        too_many_requests = (
            isinstance(response, dict)
            and "data" in response
            and response["data"] is None
            and "error" in response
            and response["error"] is None
        )
        eikon_not_running = (
            isinstance(response, dict)
            and "data" in response
            and response["data"] is None
            and "error" in response
            and response["error"] is not None
            and "Eikon Proxy not running or cannot be reached."
            in response["error"]["message"]
        )
        if too_many_requests or eikon_not_running:
            remove_object(bucket_name, object_name)
            print("remove_object", bucket_name, object_name)

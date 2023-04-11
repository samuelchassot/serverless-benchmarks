import datetime, json, os, uuid

from PIL import Image
import torch
from torchvision import transforms
from torchvision.models import resnet50
from torchvision.models import ResNet

from . import storage

client = storage.storage.get_instance()

SCRIPT_DIR: str = os.path.abspath(os.path.join(os.path.dirname(__file__)))
class_idx: dict[str, list[str]] = json.load(
    open(os.path.join(SCRIPT_DIR, "imagenet_class_index.json"), "r")
)
idx2label: list[str] = [class_idx[str(k)][1] for k in range(len(class_idx))]
model = None


def handler(event):
    model_bucket: str = event.get("bucket").get("model")
    input_bucket: str = event.get("bucket").get("input")
    key: str = event.get("object").get("input")
    model_key: str = event.get("object").get("model")
    download_path: str = "/tmp/{}-{}".format(key, uuid.uuid4())

    image_download_begin: datetime.datetime = datetime.datetime.now()
    image_path = download_path
    client.download(input_bucket, key, download_path)
    image_download_end: datetime.datetime = datetime.datetime.now()

    global model
    if not model:
        model_download_begin: datetime.datetime = datetime.datetime.now()
        model_path: str = os.path.join("/tmp", model_key)
        client.download(model_bucket, model_key, model_path)
        model_download_end: datetime.datetime = datetime.datetime.now()
        model_process_begin: datetime.datetime = datetime.datetime.now()
        model: ResNet = resnet50(pretrained=False)
        model.load_state_dict(torch.load(model_path))
        model.eval()
        model_process_end: datetime.datetime = datetime.datetime.now()
    else:
        model_download_begin: datetime.datetime = datetime.datetime.now()
        model_download_end: datetime.datetime = model_download_begin
        model_process_begin: datetime.datetime = datetime.datetime.now()
        model_process_end: datetime.datetime = model_process_begin

    process_begin: datetime.datetime = datetime.datetime.now()
    input_image: Image.Image = Image.open(image_path)
    preprocess: transforms.Compose = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    input_tensor: transforms.Compose = preprocess(input_image)
    input_batch = input_tensor.unsqueeze(
        0
    )  # create a mini-batch as expected by the model
    output: torch.Tensor = model(input_batch)
    index: int
    _, index = torch.max(output, 1)
    # The output has unnormalized scores. To get probabilities, you can run a softmax on it.
    prob: torch.Tensor = torch.nn.functional.softmax(output[0], dim=0)
    _, indices = torch.sort(output, descending=True)
    ret: str = idx2label[index]
    process_end: datetime.datetime = datetime.datetime.now()

    download_time: float = (
        image_download_end - image_download_begin
    ) / datetime.timedelta(microseconds=1)
    model_download_time: float = (
        model_download_end - model_download_begin
    ) / datetime.timedelta(microseconds=1)
    model_process_time: float = (
        model_process_end - model_process_begin
    ) / datetime.timedelta(microseconds=1)
    process_time: float = (process_end - process_begin) / datetime.timedelta(
        microseconds=1
    )
    return {
        "result": {"idx": index.item(), "class": ret},
        "measurement": {
            "download_time": download_time + model_download_time,
            "compute_time": process_time + model_process_time,
            "model_time": model_process_time,
            "model_download_time": model_download_time,
        },
    }

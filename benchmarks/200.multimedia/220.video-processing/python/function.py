#!/usr/bin/env python

import datetime
import os
import stat
import subprocess


from . import storage

client = storage.storage.get_instance()

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))


def call_ffmpeg(args) -> None:
    ret = subprocess.run(
        [os.path.join(SCRIPT_DIR, "ffmpeg", "ffmpeg"), "-y"] + args,
        # subprocess might inherit Lambda's input for some reason
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if ret.returncode != 0:
        print("Invocation of ffmpeg failed!")
        print("Out: ", ret.stdout.decode("utf-8"))
        raise RuntimeError()


# https://superuser.com/questions/556029/how-do-i-convert-a-video-to-gif-using-ffmpeg-with-reasonable-quality
def to_gif(video, duration, event) -> str:
    output = "/tmp/processed-{}.gif".format(os.path.basename(video))
    call_ffmpeg(
        [
            "-i",
            video,
            "-t",
            "{0}".format(duration),
            "-vf",
            "fps=10,scale=320:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
            "-loop",
            "0",
            output,
        ]
    )
    return output


# https://devopstar.com/2019/01/28/serverless-watermark-using-aws-lambda-layers-ffmpeg/
def watermark(video, duration, event) -> str:
    output = "/tmp/processed-{}".format(os.path.basename(video))
    watermark_file = os.path.dirname(os.path.realpath(__file__))
    call_ffmpeg(
        [
            "-i",
            video,
            "-i",
            os.path.join(watermark_file, os.path.join("resources", "watermark.png")),
            "-t",
            "{0}".format(duration),
            "-filter_complex",
            "overlay=main_w/2-overlay_w/2:main_h/2-overlay_h/2",
            output,
        ]
    )
    return output


def transcode_mp3(video, duration, event) -> None:
    pass


operations = {"transcode": transcode_mp3, "extract-gif": to_gif, "watermark": watermark}


def handler(event):
    input_bucket: str = event.get("bucket").get("input")
    output_bucket: str = event.get("bucket").get("output")
    key: str = event.get("object").get("key")
    duration: int = event.get("object").get("duration")
    op: str = event.get("object").get("op")
    download_path: str = "/tmp/{}".format(key)

    # Restore executable permission
    ffmpeg_binary: str = os.path.join(
        SCRIPT_DIR, "ffmpeg", "ffmpeg"
    )  # Memory = A(SCRIPT_DIR, "ffmpeg", "ffmpeg")
    # needed on Azure but read-only filesystem on AWS
    try:
        st: os.stat_result = os.stat(ffmpeg_binary)  # Memory = B(ffmpeg_binary)
        os.chmod(
            ffmpeg_binary, st.st_mode | stat.S_IEXEC
        )  # Memoty = C(ffmpeg_binary, st.st_mode)
    except OSError:
        pass

    download_begin: datetime.datetime = datetime.datetime.now()  # Memory = D()
    client.download(
        input_bucket, key, download_path
    )  # Memory = E(input_bucket, key, download_path)
    download_size: int = os.path.getsize(download_path)  # Memory = F(download_path)
    download_stop: datetime.datetime = datetime.datetime.now()

    process_begin: datetime.datetime = datetime.datetime.now()
    upload_path: str = operations[op](download_path, duration, event)
    process_end: datetime.datetime = datetime.datetime.now()

    upload_begin: datetime.datetime = datetime.datetime.now()
    filename: str = os.path.basename(upload_path)  # Memory = G(upload_path)
    upload_size: int = os.path.getsize(upload_path)  # Memory = F(upload_path)
    client.upload(
        output_bucket, filename, upload_path
    )  # Memory = H(output_bucket, filename, upload_path)
    upload_stop: datetime.datetime = datetime.datetime.now()

    download_time: float = (download_stop - download_begin) / datetime.timedelta(
        microseconds=1
    )
    upload_time: float = (upload_stop - upload_begin) / datetime.timedelta(
        microseconds=1
    )
    process_time: float = (process_end - process_begin) / datetime.timedelta(
        microseconds=1
    )
    return {
        "result": {"bucket": output_bucket, "key": filename},
        "measurement": {
            "download_time": download_time,
            "download_size": download_size,
            "upload_time": upload_time,
            "upload_size": upload_size,
            "compute_time": process_time,
        },
    }

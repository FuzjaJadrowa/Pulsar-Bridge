import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import zipfile
from unittest import mock

from PIL import Image

from core.media.probe import probe_media
from core.tasking import CancelToken, TaskCancelledError, TaskContext
from features.compression.handler import CompressHandler
from features.conversion.handler import ConvertHandler, ConvertMetadataHandler
from features.download.handler import DownloadHandler

def _create_animated_frames():
    colors = [
        (255, 0, 0, 255),
        (0, 255, 0, 255),
        (0, 0, 255, 255),
    ]
    frames = []
    for color in colors:
        img = Image.new("RGBA", (32, 32), (0, 0, 0, 0))
        for x in range(4, 28):
            for y in range(13, 19):
                img.putpixel((x, y), color)
        for x in range(13, 19):
            for y in range(4, 28):
                img.putpixel((x, y), color)
        frames.append(img)
    return frames

class ConversionAndCompressionTests(unittest.TestCase):
    def test_c04_animated_gif_webp_and_apng_compression_preserves_all_frames(self):
        durations = [80, 120, 160]
        formats = [
            ("gif", "GIF"),
            ("webp", "WEBP"),
            ("png", "PNG"),
        ]
        with tempfile.TemporaryDirectory(prefix="pulsar-c04-") as tmp:
            for ext, pil_format in formats:
                with self.subTest(format=ext):
                    src = os.path.join(tmp, f"anim.{ext}")
                    dst = os.path.join(tmp, f"anim_compressed.{ext}")
                    frames = _create_animated_frames()
                    try:
                        save_kwargs = {
                            "save_all": True,
                            "append_images": frames[1:],
                            "duration": durations,
                            "loop": 0,
                        }
                        if pil_format == "GIF":
                            save_kwargs["disposal"] = 2
                        frames[0].save(src, format=pil_format, **save_kwargs)
                    finally:
                        for f in frames:
                            f.close()

                    meta_events = []
                    meta_ctx = TaskContext(f"meta-{ext}", meta_events.append)
                    ConvertMetadataHandler(f"meta-{ext}", task_context=meta_ctx).run([src])
                    self.assertEqual(len(meta_events), 1)
                    self.assertEqual(meta_events[0]["type"], "metadata")
                    self.assertTrue(meta_events[0]["data"]["is_animated"])
                    self.assertEqual(meta_events[0]["data"]["frame_count"], 3)

                    comp_events = []
                    comp_ctx = TaskContext(f"comp-{ext}", comp_events.append)
                    CompressHandler(f"comp-{ext}", task_context=comp_ctx).run(
                        [],
                        payload={
                            "input_path": src,
                            "output_path": dst,
                            "output_format": ext,
                            "category": "image",
                            "compress_mode": "percent",
                            "target_percent": 60.0,
                            "is_animated": True,
                            "ffmpeg_path": "",
                            "ffmpeg_args": [],
                        },
                    )
                    self.assertEqual(len(comp_events), 1)
                    self.assertEqual(comp_events[0]["type"], "finished")
                    self.assertTrue(
                        comp_events[0]["success"],
                        f"Animated {ext} compression failed: {comp_events[0]}",
                    )
                    self.assertTrue(os.path.isfile(dst))

                    with Image.open(dst) as out_img:
                        self.assertTrue(getattr(out_img, "is_animated", False))
                        self.assertEqual(
                            getattr(out_img, "n_frames", 1),
                            3,
                            f"Animated {ext} lost frames during compression!",
                        )
                        out_img.seek(0)
                        corner_alpha = out_img.convert("RGBA").getpixel((0, 0))[3]
                        self.assertEqual(
                            corner_alpha,
                            0,
                            f"Animated {ext} lost corner transparency!",
                        )
                        self.assertEqual(out_img.info.get("loop", 0), 0)

    def test_c02_ffprobe_metadata_and_real_conversion_codecs(self):
        ffmpeg_bin = shutil.which("ffmpeg")
        ffprobe_bin = shutil.which("ffprobe")
        if not ffmpeg_bin or not ffprobe_bin:
            self.skipTest("ffmpeg/ffprobe not available on PATH")

        with tempfile.TemporaryDirectory(prefix="pulsar-c02-") as tmp:
            ogv_path = os.path.join(tmp, "sample.ogv")
            gen_ogv = subprocess.run(
                [
                    ffmpeg_bin,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-y",
                    "-f",
                    "lavfi",
                    "-i",
                    "color=c=red:s=64x64:d=0.3:r=10",
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=440:duration=0.3",
                    "-c:v",
                    "libtheora",
                    "-c:a",
                    "libvorbis",
                    ogv_path,
                ],
                capture_output=True,
                text=True,
            )
            if gen_ogv.returncode != 0:
                self.skipTest(f"FFmpeg build lacks libtheora/libvorbis: {gen_ogv.stderr}")

            meta_events = []
            meta_ctx = TaskContext("meta-ogv", meta_events.append)
            ConvertMetadataHandler("meta-ogv", task_context=meta_ctx).run(
                ["--ffprobe-location", ffprobe_bin, ogv_path]
            )
            self.assertEqual(len(meta_events), 1)
            self.assertEqual(meta_events[0]["type"], "metadata")
            data = meta_events[0]["data"]
            self.assertEqual(data["video_codec"], "theora")
            self.assertEqual(data["audio_codec"], "vorbis")
            self.assertEqual(len(data["streams"]), 2)
            self.assertIsNotNone(data["streams"][0]["time_base"])
            self.assertIsNotNone(data["streams"][0]["pix_fmt"])
            self.assertEqual(data["streams"][1]["channels"], 1)

            cached = probe_media(ogv_path, ffprobe_path=ffprobe_bin)
            self.assertEqual(cached["streams"][0]["codec_name"], "theora")

            webm_path = os.path.join(tmp, "out.webm")
            conv_events = []
            conv_ctx = TaskContext("conv-webm", conv_events.append)
            ConvertHandler("conv-webm", task_context=conv_ctx).run(
                [],
                payload={
                    "input_path": ogv_path,
                    "output_path": webm_path,
                    "output_format": "webm",
                    "category": "video",
                    "source_duration_seconds": 0.3,
                    "ffmpeg_path": ffmpeg_bin,
                    "ffmpeg_args": [
                        "-hide_banner",
                        "-y",
                        "-i",
                        ogv_path,
                        "-map",
                        "0:v?",
                        "-map",
                        "0:a?",
                        "-c:v",
                        "libvpx-vp9",
                        "-c:a",
                        "copy",
                        webm_path,
                    ],
                },
            )
            self.assertEqual(len(conv_events), 1)
            self.assertTrue(conv_events[0]["success"], conv_events[0])
            self.assertTrue(os.path.isfile(webm_path))

            out_probe = probe_media(webm_path, ffprobe_path=ffprobe_bin)
            codecs = {s["codec_type"]: s["codec_name"] for s in out_probe["streams"]}
            self.assertEqual(codecs.get("video"), "vp9")
            self.assertEqual(codecs.get("audio"), "vorbis")

    def test_archive_conversion_and_traversal_security(self):
        with tempfile.TemporaryDirectory(prefix="pulsar-arc-") as tmp:
            good_zip = os.path.join(tmp, "good.zip")
            with zipfile.ZipFile(good_zip, "w") as zf:
                zf.writestr("nested/hello.txt", "pulsar-bridge-ok")

            out_targz = os.path.join(tmp, "out.tar.gz")
            events = []
            ctx = TaskContext("arc-1", events.append)
            ConvertHandler("arc-1", task_context=ctx).run(
                [],
                payload={
                    "input_path": good_zip,
                    "output_path": out_targz,
                    "output_format": "tar.gz",
                    "category": "archive",
                },
            )
            self.assertEqual(len(events), 1)
            self.assertTrue(events[0]["success"], events[0])
            with tarfile.open(out_targz, "r:gz") as tf:
                names = tf.getnames()
                self.assertIn("nested/hello.txt", names)

            evil_zip = os.path.join(tmp, "evil.zip")
            with zipfile.ZipFile(evil_zip, "w") as zf:
                zf.writestr("../escape.txt", "malicious")

            evil_out = os.path.join(tmp, "evil.tar.gz")
            evil_events = []
            evil_ctx = TaskContext("arc-evil", evil_events.append)
            ConvertHandler("arc-evil", task_context=evil_ctx).run(
                [],
                payload={
                    "input_path": evil_zip,
                    "output_path": evil_out,
                    "output_format": "tar.gz",
                    "category": "archive",
                },
            )
            self.assertEqual(len(evil_events), 1)
            self.assertIs(evil_events[0]["success"], False)
            self.assertIn("Unsafe path", evil_events[0]["error"])

    def test_offline_download_handler_with_stubbed_extractor_and_cooperative_cancel(self):
        class FakeYDL:
            def __init__(self, opts, trigger_cancel=None):
                self.opts = opts
                self._pps = {}
                self.trigger_cancel = trigger_cancel

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def download(self, urls):
                hooks = self.opts.get("progress_hooks", [])
                for hook in hooks:
                    hook({
                        "status": "downloading",
                        "downloaded_bytes": 50,
                        "total_bytes": 100,
                        "eta": 1,
                        "speed": 50,
                        "filename": "clip.mp4",
                    })
                    if self.trigger_cancel is not None:
                        self.trigger_cancel()
                        hook({
                            "status": "downloading",
                            "downloaded_bytes": 75,
                            "total_bytes": 100,
                            "eta": 1,
                            "speed": 50,
                            "filename": "clip.mp4",
                        })
                return 0

        events = []
        ctx = TaskContext("dl-ok", events.append)
        with mock.patch("features.download.handler.create_resolver_ydl", side_effect=lambda opts: FakeYDL(opts)):
            with mock.patch("features.download.handler.emit_json"):
                DownloadHandler("dl-ok", task_context=ctx).run(["https://example.test/video"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "finished")
        self.assertTrue(events[0]["success"])

        token = CancelToken()
        cancel_events = []
        cancel_ctx = TaskContext("dl-cancel", cancel_events.append, cancel_token=token)
        with mock.patch(
            "features.download.handler.create_resolver_ydl",
            side_effect=lambda opts: FakeYDL(opts, trigger_cancel=token.cancel),
        ):
            with mock.patch("features.download.handler.emit_json"):
                with self.assertRaises(TaskCancelledError):
                    DownloadHandler("dl-cancel", task_context=cancel_ctx).run(["https://example.test/video"])
        self.assertEqual(cancel_events, [])

if __name__ == "__main__":
    unittest.main()
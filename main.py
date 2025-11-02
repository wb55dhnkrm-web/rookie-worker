import os, tempfile, subprocess
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import boto3
from botocore.client import Config

S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(signature_version="s3v4"))

app = FastAPI(title="Rookie Worker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def presign_get(key: str, expires=3600):
  return s3.generate_presigned_url("get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=expires)

def run_ffmpeg_dummy(in_path: str, out_path: str):
  # This is just a test processor – we’ll replace it later
  subprocess.run([
    "ffmpeg","-y","-i",in_path,"-t","5",
    "-c:v","libx264","-c:a","aac","-movflags","+faststart",out_path
  ], check=True)

@app.get("/health")
def health():
  return {"ok": True}

@app.post("/process")
def process_job(payload: dict):
  s3_key = payload["s3_key"]
  out_prefix = payload.get("out_prefix", "results/demo/")

  with tempfile.TemporaryDirectory() as td:
    in_path = os.path.join(td, "input.mp4")
    outdir = os.path.join(td, "out"); os.makedirs(outdir, exist_ok=True)

    # 1) Download video from S3
    s3.download_file(S3_BUCKET, s3_key, in_path)

    # 2) Process
    out_mp4 = os.path.join(outdir, "highlights.mp4")
    run_ffmpeg_dummy(in_path, out_mp4)

    # 3) Upload results back to S3
    artifacts = []
    for name in os.listdir(outdir):
      loc = os.path.join(outdir, name)
      key = f"{out_prefix}{name}"
      s3.upload_file(loc, S3_BUCKET, key, ExtraArgs={"ContentType": "video/mp4"})
      artifacts.append({"name": name, "key": key, "url": presign_get(key)})

    return {"status": "done", "artifacts": artifacts}

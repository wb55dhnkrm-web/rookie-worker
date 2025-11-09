import os, tempfile, subprocess
import logging                                   # ⭐ ADDED
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# -----------------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------------
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")

s3 = boto3.client("s3", region_name=AWS_REGION, config=Config(signature_version="s3v4"))

# -----------------------------------------------------------------------------------
# ⭐ ADDED: log the AWS identity once at startup (proves which IAM user/role Render uses)
# -----------------------------------------------------------------------------------
def log_aws_identity():
    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        ident = sts.get_caller_identity()
        logging.info(f"🟢 AWS Identity: {ident}")       # shows Account + Arn in Render logs
    except Exception as e:
        logging.exception("❌ STS identity check failed")

log_aws_identity()  # run once at import time

# -----------------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------------
app = FastAPI(title="Rookie Worker")

# (optional) You can restrict CORS to your Vercel domain(s) instead of "*"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # e.g. ["https://your-vercel-domain.vercel.app"]
    allow_methods=["*"],
    allow_headers=["*"],
)

def presign_get(key: str, expires=3600):
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires
    )

def run_ffmpeg_fast(in_path: str, out_path: str):
    # Very fast: trim to 3 seconds without re-encoding (avoids timeouts)
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-ss", "0", "-i", in_path,
            "-t", "3",
            "-c", "copy",
            out_path
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

@app.get("/health")
def health():
    return {"ok": True}

# ⭐ ADDED: quick endpoint to confirm credentials at runtime if needed
@app.get("/debug/identity")
def debug_identity():
    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        return {"ok": True, "identity": sts.get_caller_identity()}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/exists")
def debug_exists(s3_key: str):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return {"exists": True}
    except ClientError as e:
        return {"exists": False, "error": str(e)}

@app.post("/process")
def process_job(payload: dict):
    try:
        s3_key = payload["s3_key"]
        out_prefix = payload.get("out_prefix", "results/demo/")

        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, "input.mp4")
            outdir = os.path.join(td, "out")
            os.makedirs(outdir, exist_ok=True)

            # 1) Download from S3 (will throw if key is wrong / permissions missing)
            s3.download_file(S3_BUCKET, s3_key, in_path)

            # 2) Process very fast (3s copy)
            out_mp4 = os.path.join(outdir, "highlights.mp4")
            run_ffmpeg_fast(in_path, out_mp4)

            # 3) Upload results
            artifacts = []
            for name in os.listdir(outdir):
                loc = os.path.join(outdir, name)
                key = f"{out_prefix}{name}"
                s3.upload_file(
                    loc, S3_BUCKET, key,
                    ExtraArgs={
                        "ContentType": "video/mp4" if name.endswith(".mp4") else "application/octet-stream"
                    }
                )
                artifacts.append({"name": name, "key": key, "url": presign_get(key)})

            return {"status": "done", "artifacts": artifacts}

    except ClientError as e:
        return {"status": "error", "where": "s3", "message": str(e)}
    except FileNotFoundError as e:
        return {"status": "error", "where": "ffmpeg_or_path", "message": str(e)}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "where": "ffmpeg", "message": e.stderr.decode("utf-8", errors="ignore")}
    except Exception as e:
        return {"status": "error", "where": "unknown", "message": str(e)}

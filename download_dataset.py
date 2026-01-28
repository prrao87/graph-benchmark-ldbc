import hashlib
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

URL = "https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst"

BASE = Path(__file__).resolve().parent
ARCHIVE = BASE / Path(urlparse(URL).path).name
CSV_DIR = BASE / "csv"

# If GDC publishes a checksum file for this artifact you can paste it here.
# Otherwise, leave as None and optionally print the computed hash after download.
EXPECTED_SHA256 = None  # e.g. "deadbeef..."


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    if CSV_DIR.exists():
        print("csv/ already exists; skipping.")
        return 0

    if shutil.which("curl") is None:
        print("ERROR: curl not found.", file=sys.stderr)
        return 1

    print(f"Downloading {URL}")
    # -C - resumes; -L follows redirects; --fail fails on HTTP errors
    subprocess.check_call(["curl", "-L", "--fail", "-C", "-", "-o", str(ARCHIVE), URL])

    got = sha256(ARCHIVE)
    print(f"SHA256: {got}")
    if EXPECTED_SHA256 and got.lower() != EXPECTED_SHA256.lower():
        print("ERROR: checksum mismatch", file=sys.stderr)
        return 2

    # Extract
    if shutil.which("tar") is None:
        print("ERROR: tar not found.", file=sys.stderr)
        return 1

    tmp = BASE / "_extract_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    if shutil.which("unzstd") is not None:
        subprocess.check_call(
            ["tar", "-xf", str(ARCHIVE), "--use-compress-program=unzstd", "-C", str(tmp)]
        )
    elif shutil.which("zstd") is not None:
        # fallback: zstd -d -c | tar -xf -
        zstd = subprocess.Popen(["zstd", "-d", "-c", str(ARCHIVE)], stdout=subprocess.PIPE)
        try:
            subprocess.check_call(["tar", "-xf", "-", "-C", str(tmp)], stdin=zstd.stdout)
        finally:
            if zstd.stdout:
                zstd.stdout.close()
            zstd.wait()
    else:
        print("ERROR: need `unzstd` or `zstd` to extract .tar.zst", file=sys.stderr)
        return 1

    # Move single top-level dir to csv/
    dirs = [p for p in tmp.iterdir() if p.is_dir()]
    if len(dirs) == 1:
        dirs[0].replace(CSV_DIR)
    else:
        CSV_DIR.mkdir()
        for p in tmp.iterdir():
            p.replace(CSV_DIR / p.name)

    shutil.rmtree(tmp, ignore_errors=True)
    print("Done → ./csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

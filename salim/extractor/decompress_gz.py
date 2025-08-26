# decompress_gz.py
import gzip, shutil

src = "input/victory_branch1_promo.gz"
dst = "input/victory_branch1_promo.xml"

with gzip.open(src, "rb") as f_in:
    with open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

print(f"Decompressed {src} → {dst}")

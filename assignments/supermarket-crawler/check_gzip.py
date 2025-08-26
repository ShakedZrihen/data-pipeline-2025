import gzip, sys
ok=0; bad=[]
for p in sys.argv[1:]:
    try:
        with gzip.open(p,'rb') as f:
            f.read(64)   # just try to read a few bytes
        print(f'OK  {p}')
        ok+=1
    except OSError as e:
        print(f'BAD {p}: {e}')
        bad.append(p)
print(f'\nSummary: {ok} OK, {len(bad)} bad')

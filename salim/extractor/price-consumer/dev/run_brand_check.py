import sys, types, importlib.util

# Stub heavy deps to allow importing the Lambda module locally
sys.modules['boto3'] = types.SimpleNamespace(client=lambda name: None)
sys.modules['pg_resilient'] = types.SimpleNamespace(from_env=lambda: None)
sys.modules['pg8000'] = types.SimpleNamespace(dbapi=types.SimpleNamespace(connect=lambda **k: None))
psycopg2_extras = types.ModuleType('psycopg2.extras')
sys.modules['psycopg2.extras'] = psycopg2_extras
sys.modules['psycopg2'] = types.SimpleNamespace(extras=psycopg2_extras, connect=lambda **k: None)

path = 'salim/extractor/price-consumer/consumer_lambda.py'
spec = importlib.util.spec_from_file_location('consumer_lambda_pc', path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

samples = [
    'סכיניגילוחאדג\u2019כףסכיניגילוחהאדגכף',
    'סבונוןג\u2019ללניקויאססנו',
]

for s in samples:
    eb, cn = mod.extract_brand_and_clean_name(s)
    hb = mod.heuristic_brand_from_product(s)
    print('INPUT        :', s)
    print('alias split  :', eb, '| name ->', cn)
    print('heuristic    :', hb)
    print('---')


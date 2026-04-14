import os, json, urllib.request, urllib.parse

CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
DEV_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", os.environ.get("GOOGLE_DEVELOPER_TOKEN", "xr00fFrU4qlg4WlDlkIVfA"))
MCC_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", os.environ.get("GOOGLE_MCC_ID", "6359594317"))
CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", os.environ.get("GOOGLE_CUSTOMER_ID", "")).replace("-","")

print(f"DEV_TOKEN: {DEV_TOKEN}")
print(f"MCC_ID (login): {MCC_ID}")
print(f"CUSTOMER_ID na secret: '{CUSTOMER_ID}'")

data = urllib.parse.urlencode({"client_id":CLIENT_ID,"client_secret":CLIENT_SECRET,"refresh_token":REFRESH_TOKEN,"grant_type":"refresh_token"}).encode()
req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data, method="POST")
req.add_header("Content-Type","application/x-www-form-urlencoded")
with urllib.request.urlopen(req) as r:
    tok = json.loads(r.read())["access_token"]
print(f"Access token OK")

for cid, login in [("3180978445","6359594317"),("6359594317",None),("6359594317","6359594317")]:
    url = f"https://googleads.googleapis.com/v19/customers/{cid}/googleAds:searchStream"
    body = json.dumps({"query":"SELECT segments.date,metrics.clicks FROM campaign WHERE segments.date='2026-04-01' LIMIT 1"}).encode()
    req2 = urllib.request.Request(url, data=body, method="POST")
    req2.add_header("Authorization",f"Bearer {tok}")
    req2.add_header("developer-token",DEV_TOKEN)
    req2.add_header("Content-Type","application/json")
    if login: req2.add_header("login-customer-id",login)
    tag = f"cid={cid}" + (f" login={login}" if login else " no-login")
    try:
        with urllib.request.urlopen(req2) as r2:
            resp = r2.read().decode()[:300]
            print(f"OK [{tag}]: {resp}")
    except urllib.error.HTTPError as e:
        err = e.read().decode()[:300]
        print(f"ERROR {e.code} [{tag}]: {err}")
    except Exception as e:
        print(f"EXCEPTION [{tag}]: {str(e)[:200]}")

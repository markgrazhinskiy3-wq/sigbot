"""
Run this script once to import your Pocket Option session cookies into the bot.

Usage:
  python3 signal_bot/set_cookies.py '<json_cookie_array>'

How to export cookies from Chrome:
  1. Install extension "EditThisCookie" or "Cookie-Editor"
  2. Go to pocketoption.com and log in
  3. Open the extension, click Export (JSON format)
  4. Paste the JSON array here as a single argument
"""
import json, sys, os

COOKIES_PATH = os.path.join(os.path.dirname(__file__), "po_cookies.json")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    raw = sys.argv[1].strip()
    try:
        cookies = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        sys.exit(1)

    if not isinstance(cookies, list):
        print("Expected a JSON array of cookie objects.")
        sys.exit(1)

    # Normalize to Playwright format
    playwright_cookies = []
    for c in cookies:
        name = c.get("name") or c.get("key") or c.get("Name")
        value = c.get("value") or c.get("Value") or ""
        domain = c.get("domain") or c.get("Domain") or ".pocketoption.com"
        path = c.get("path") or c.get("Path") or "/"
        if not name:
            continue
        pc = {"name": name, "value": value, "domain": domain, "path": path}
        if "expirationDate" in c:
            pc["expires"] = int(c["expirationDate"])
        elif "expires" in c and c["expires"] not in (None, -1, ""):
            pc["expires"] = int(c["expires"])
        if "sameSite" in c:
            ss = str(c["sameSite"]).capitalize()
            if ss in ("Strict", "Lax", "None"):
                pc["sameSite"] = ss
        pc["httpOnly"] = bool(c.get("httpOnly") or c.get("HttpOnly") or False)
        pc["secure"] = bool(c.get("secure") or c.get("Secure") or False)
        playwright_cookies.append(pc)

    with open(COOKIES_PATH, "w") as f:
        json.dump(playwright_cookies, f, indent=2)

    print(f"Saved {len(playwright_cookies)} cookies to {COOKIES_PATH}")
    print("Now restart the Signal Bot — it will use these cookies automatically.")

if __name__ == "__main__":
    main()

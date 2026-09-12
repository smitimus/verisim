"""
Build-time script: strip grocery- and gas-station-only routes from api/main.py.
Produces a support-only API with a clean /docs page.

Usage: python strip_support.py <input_path> <output_path>

Does NOT modify the source file — run only inside the Docker build.
"""
import re
import sys

src = open(sys.argv[1]).read()

# Remove all grocery-only and gas-station-only sections.
# Each section begins with the standard comment header and ends just before
# the next section header (another line of dashes) — or at EOF.
pattern = (
    r'\n'
    r'# -{75}\n'
    r'# (?:Grocery|Gas-station)(?: only| —).*?\n'
    r'# -{75}\n'
    r'.*?'
    r'(?=\n# -{75}|\Z)'
)
cleaned = re.sub(pattern, '', src, flags=re.DOTALL)

# Update title + description for support branding
cleaned = cleaned.replace(
    'title="Verisim Data Generator API"',
    'title="Verisim Customer Support API"',
).replace(
    'description="Multi-industry mock data platform — Gas Station, Grocery, and more"',
    'description="Verisim Customer Support — mock contact-center data (tickets, ACD, chat, sNPS)"',
)

open(sys.argv[2], 'w').write(cleaned)
print(f"Wrote support-only API to {sys.argv[2]}")

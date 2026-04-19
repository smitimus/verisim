"""
Build-time script: strip gas-station-only routes from api/main.py.
Produces a grocery-only API with a clean /docs page.

Usage: python strip_gas_station.py <input_path> <output_path>

Does NOT modify the source file — run only inside the Docker build.
"""
import re
import sys

src = open(sys.argv[1]).read()

# Remove the gas-station-only fuel section.
# The section begins with its standard comment header and ends just before
# the next section header (another line of dashes).
# Regex: matches from the "Gas-station only" header through to (but not
# including) the next top-level section separator.
pattern = (
    r'\n'
    r'# -{75}\n'
    r'# Gas-station only:.*?\n'
    r'# -{75}\n'
    r'.*?'
    r'(?=\n# -{75})'
)
cleaned = re.sub(pattern, '', src, flags=re.DOTALL)

# Update title + description for grocery branding
cleaned = cleaned.replace(
    'title="Verisim Data Generator API"',
    'title="Verisim Grocery API"',
).replace(
    'description="Multi-industry mock data platform — Gas Station, Grocery, and more"',
    'description="Verisim Grocery — mock data platform for retail analytics"',
)

open(sys.argv[2], 'w').write(cleaned)
print(f"Wrote grocery-only API to {sys.argv[2]}")

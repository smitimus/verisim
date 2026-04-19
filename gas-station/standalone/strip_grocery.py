"""
Build-time script: strip grocery-only routes from api/main.py.
Produces a gas-station-only API with a clean /docs page.

Usage: python strip_grocery.py <input_path> <output_path>

Does NOT modify the source file — run only inside the Docker build.
"""
import re
import sys

src = open(sys.argv[1]).read()

# Remove all grocery-only sections.
# Each section begins with the standard comment header and ends just before
# the next section header (another line of dashes).
pattern = (
    r'\n'
    r'# -{75}\n'
    r'# Grocery only:.*?\n'
    r'# -{75}\n'
    r'.*?'
    r'(?=\n# -{75})'
)
cleaned = re.sub(pattern, '', src, flags=re.DOTALL)

# Update title + description for gas station branding
cleaned = cleaned.replace(
    'title="Verisim Data Generator API"',
    'title="Verisim Gas Station API"',
).replace(
    'description="Multi-industry mock data platform — Gas Station, Grocery, and more"',
    'description="Verisim Gas Station — mock data platform for fuel & convenience store analytics"',
)

open(sys.argv[2], 'w').write(cleaned)
print(f"Wrote gas-station-only API to {sys.argv[2]}")

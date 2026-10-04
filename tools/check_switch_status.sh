#!/bin/bash
# Does `switch.sh status` name every container the compose files actually create?
#
# The status branch used to detect the gas-station dev stack by a container name
# no mode creates (`^verisim-gas-station$`), so a running dev stack printed
# "none" — the one mode you would run precisely to find out what is up
# (t_a6ecb731). Docker is not always available where this runs, so the greps are
# exercised against the names declared in the compose files with a stub `docker`.
set -u
root="${1:?usage: check_switch_status.sh <verisim-checkout>}"
rc=0

# Extract every container_name: from an industry's compose files.
names_for() {
    grep -h 'container_name:' "$root/$1"/*.yaml 2>/dev/null \
        | sed 's/.*container_name:[[:space:]]*//' | tr -d '"'"'" \
        | sort -u
}

for industry in grocery gas-station; do
    names=$(names_for "$industry")
    [ -n "$names" ] || { echo "$industry: FAIL — no container_name found in $industry/*.yaml"; rc=1; continue; }

    for name in $names; do
        # Replay switch.sh's status greps against a fake `docker ps` listing.
        matched="none"
        if echo "$name" | grep -q "verisim-${industry}-dev"; then matched="dev"
        elif echo "$name" | grep -q "verisim-${industry}-test"; then matched="test"
        elif echo "$name" | grep -q "^verisim-${industry}\$"; then matched="release"
        fi

        if [ "$matched" = "none" ]; then
            echo "$industry: FAIL — switch.sh status reports 'none' for a container it creates: $name"
            rc=1
        else
            echo "$industry: OK — $name detected as '$matched'"
        fi
    done
done

exit $rc

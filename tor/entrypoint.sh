#!/bin/sh
set -e

# Hache le mot de passe du port de contrôle (variable d'environnement)
PASS="${TOR_CONTROL_PASSWORD:-tor_control_secret}"
HASH=$(tor --hash-password "$PASS" | tail -n 1)

# Le hash est passé en argument CLI (pas d'écriture dans /etc/tor/torrc,
# qui appartient à root). "$@" permet de surcharger des options par service
# (ex. tor-notaire fige son circuit pour garder une IP stable face au F5).
exec tor -f /etc/tor/torrc HashedControlPassword "$HASH" "$@"

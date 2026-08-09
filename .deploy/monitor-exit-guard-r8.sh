#!/usr/bin/env bash
set -Eeuo pipefail

readonly TRANSACTION=/home/smith/.local/state/acp-cutover/frozen-0b94403-r8
readonly PHASE=${TRANSACTION}/phase
readonly EVIDENCE=${TRANSACTION}/strict-live-proof.json
readonly STATUS=${TRANSACTION}/rollout-status.json
result="${1:-unknown}"

strict_success() {
    [[ "${result}" = success ]] && python3 - "${PHASE}" "${EVIDENCE}" "${STATUS}" <<'PY'
import json
import sys

phase, evidence_path, status_path = sys.argv[1:]
try:
    evidence = json.load(open(evidence_path, encoding="utf-8"))
    status = json.load(open(status_path, encoding="utf-8"))
    valid = (
        open(phase, encoding="ascii").read().strip() == "validation_passed"
        and evidence.get("status") == "success"
        and evidence.get("correlated_live_chains", 0) > 0
        and evidence.get("release_integrity_valid") is True
        and evidence.get("installed_config_attestation_valid") is True
        and evidence.get("telegram_render_verified") is True
        and evidence.get("verified_telegram_final_parts", 0) > 0
        and evidence.get("herdr_restarted") is False
        and evidence.get("historical_recovery") is False
        and status.get("state") == "success"
        and status.get("phase") == "validation_passed"
        and status.get("herdr_restarted") is False
        and status.get("historical_recovery") is False
    )
except (OSError, ValueError):
    valid = False
raise SystemExit(0 if valid else 1)
PY
}

if strict_success; then
    exit 0
fi

printf 'validation_failed\n' >"${PHASE}.tmp.$$"
chmod 600 "${PHASE}.tmp.$$"
mv -f "${PHASE}.tmp.$$" "${PHASE}"
# Never synchronously stop a unit from the ExecStopPost of a unit ordered after
# it.  The rollback unit owns conflicts, ordering, and verified restoration.
systemctl --user start --no-block acp-r8-rollback.service >/dev/null 2>&1 || true
exit 0

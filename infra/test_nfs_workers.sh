#!/usr/bin/env bash
set -u
# test_nfs_workers.sh
# Usage:
#   ./test_nfs_workers.sh hosts.txt
# Optional:
#   ATTEMPT_FIX=1 ./test_nfs_workers.sh hosts.txt
#
# Requisiti:
#  - ssh access to Crispinoadmin@<IP>
#  - (optional) ssh-copy-id to avoid password prompts
#
HOSTS_FILE="${1:-workers.txt}"
LOGDIR="logs"
mkdir -p "${LOGDIR}"

ATTEMPT_FIX="${ATTEMPT_FIX:-0}"   # set to 1 to attempt mkdir/chown via sudo on remote

if [[ ! -f "${HOSTS_FILE}" ]]; then
  echo "Hosts file '${HOSTS_FILE}' not found. Create it with one IP per line."
  exit 2
fi

echo "=== Test NFS shared path on hosts listed in ${HOSTS_FILE} ==="
echo "Logs will be written to ${LOGDIR}/<host>.log"
echo

summary_file="${LOGDIR}/summary.txt"
: > "${summary_file}"

while IFS= read -r host || [[ -n "$host" ]]; do
  host="$(echo "$host" | tr -d '[:space:]')"
  [[ -z "$host" ]] && continue

  echo "---- HOST: ${host} ----"
  logfile="${LOGDIR}/${host}.log"
  : > "${logfile}"

  # Remote script executed on each host; use single-quoted heredoc so expansion happens remotely.
  ssh -o ConnectTimeout=12 -o StrictHostKeyChecking=accept-new "Crispinoadmin@${host}" 'bash -s' <<'REMOTE' >"${logfile}" 2>&1
echo "REMOTE HOST: $(hostname)"
echo "date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Trying to inspect /data ..."
echo "ls -ld /data"
ls -ld /data 2>/dev/null || echo "/data not present or cannot list"
echo
echo "ls -ld /data/crm_with_event_time"
ls -ld /data/crm_with_event_time 2>/dev/null || echo "/data/crm_with_event_time not present"
echo
echo "ls -ld /data/crm_with_event_time/header"
ls -ld /data/crm_with_event_time/header 2>/dev/null || echo "/data/crm_with_event_time/header not present"
echo
# Try to create the dir (non-fatal). If ATTEMPT_FIX env var is set on the local wrapper, we will attempt sudo below.
if [[ "${ATTEMPT_FIX_LOCAL:-0}" == "1" ]]; then
  echo "ATTEMPT_FIX enabled: trying mkdir -p and chown (via sudo) on /data/crm_with_event_time/header ..."
  sudo mkdir -p /data/crm_with_event_time/header 2>/dev/null || echo "sudo mkdir failed or not permitted"
  sudo chown -R Crispinoadmin:Crispinoadmin /data/crm_with_event_time 2>/dev/null || echo "sudo chown failed or not permitted"
fi
echo "Attempting touch test file ..."
if mkdir -p /data/crm_with_event_time/header 2>/dev/null && touch /data/crm_with_event_time/header/test_from_$(hostname).txt 2>/dev/null; then
  echo "TOUCH_OK: created /data/crm_with_event_time/header/test_from_$(hostname).txt"
else
  echo "TOUCH_FAILED (no permission or path missing). Showing /data stat and perms:"
  stat -c "mode=%a uid=%u gid=%g owner=%U:%G" /data 2>/dev/null || true
  echo "listing /data:"
  ls -ld /data 2>/dev/null || true
fi
echo
echo "Listing target dir after touch attempt:"
ls -l /data/crm_with_event_time/header 2>/dev/null || true
echo
echo "Disk usage for /data:"
df -h /data 2>/dev/null || true
echo
echo "Mount info containing /data:"
mount | grep " /data " || true
REMOTE

  # if ATTEMPT_FIX requested by caller, pass the env var to remote via wrapper
  # Note: remote ATTEMPT_FIX used above is read from ATTEMPT_FIX_LOCAL env var forwarded by ssh below (see instructions).
  if [[ "${ATTEMPT_FIX}" == "1" ]]; then
    # re-run with ATTEMPT_FIX_LOCAL exported to remote via environment variable forwarding
    # (some SSH servers disallow forwarding env vars; fallback below.)
    echo "Re-running attempt with sudo-fix on ${host} (output appended to ${logfile})..."
    ssh -o ConnectTimeout=12 -o StrictHostKeyChecking=accept-new -o SendEnv=ATTEMPT_FIX_LOCAL "Crispinoadmin@${host}" 'bash -s' <<'REMOTE' >>"${logfile}" 2>&1
export ATTEMPT_FIX_LOCAL=1
# same remote block as above
echo "REMOTE HOST: $(hostname) (attempt-fix run)"
echo "Trying sudo mkdir/chown (may prompt for password)..."
sudo mkdir -p /data/crm_with_event_time/header 2>/dev/null && sudo chown -R Crispinoadmin:Crispinoadmin /data/crm_with_event_time 2>/dev/null && echo "sudo mkdir+chown OK" || echo "sudo mkdir/chown failed or not permitted"
echo "Then trying touch ..."
mkdir -p /data/crm_with_event_time/header 2>/dev/null && touch /data/crm_with_event_time/header/test_from_$(hostname).txt 2>/dev/null && echo "TOUCH_OK after sudo-fix" || echo "TOUCH still FAILED"
REMOTE
  fi

  # collect summary line
  if grep -q "TOUCH_OK" "${logfile}"; then
    status="OK"
  elif grep -q "TOUCH_OK after sudo-fix" "${logfile}"; then
    status="OK(sudo)"
  else
    status="FAILED"
  fi
  echo -e "${host}\t${status}" | tee -a "${summary_file}"
  echo "log -> ${logfile}"
  echo
done < "${HOSTS_FILE}"

echo
echo "=== Summary ==="
cat "${summary_file}"
echo "Detailed logs in ${LOGDIR}/"

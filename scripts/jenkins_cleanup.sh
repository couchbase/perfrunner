# Usage: jenkins_cleanup.sh <directory> <days> [--dry-run]
# Deletes archive contents for numeric-named directories older than <days> days.
# Pass --dry-run to simulate without deleting anything.
if [[ -z "$1" ]] || [[ -z "$2" ]]; then
  echo "Usage: $(basename "$0") <directory> <days> [--dry-run]" >&2
  exit 1
fi
target_dir="$1"
if [[ ! -d "$target_dir" ]]; then
  echo "Error: directory '$target_dir' does not exist" >&2
  exit 1
fi
days="$2"
if ! [[ "$days" =~ ^[0-9]+$ ]]; then
  echo "Error: <days> must be a positive integer" >&2
  exit 1
fi
dry_run=false
if [[ "$3" == "--dry-run" ]]; then
  dry_run=true
  echo "[Dry run] No files will be deleted."
fi

cutoff=$(date -d "${days} days ago" +%s)
total_bytes=0
deleted_count=0
for dir in "$target_dir"/*/; do
  basename=$(basename "$dir")
  if [[ "$basename" =~ ^[0-9]+$ ]] && [[ -d "$dir" ]]; then
    # Try birth time first (%W); fall back to change time (%Z) if unsupported (returns 0 or ?)
    creation_time=$(stat -c "%W" "$dir" 2>/dev/null)
    if [[ -z "$creation_time" ]] || [[ "$creation_time" == "0" ]] || [[ "$creation_time" == "?" ]]; then
      creation_time=$(stat -c "%Z" "$dir" 2>/dev/null)
    fi
    if [[ -n "$creation_time" ]] && (( creation_time < cutoff )); then
      archive_dir="$dir/archive"
      if [[ -d "$archive_dir" ]]; then
        archive_bytes=$(du -sb "$archive_dir" 2>/dev/null | cut -f1)
        (( total_bytes += archive_bytes ))
        (( deleted_count++ ))
        if [[ "$dry_run" == false ]]; then
          rm -rf "${archive_dir:?}/"*
        fi
      fi
    fi
  fi
done

# Print summary
[[ "$dry_run" == true ]] && label="would be freed" || label="freed"
if [[ "$dry_run" == false ]]; then
  echo "Archives deleted: ${deleted_count} build(s)"
else
  echo "Archives that would be deleted: ${deleted_count} build(s)"
fi
if (( total_bytes >= 1073741824 )); then
  printf "Total space %s: %.2f GB\n" "$label" "$(awk "BEGIN {printf \"%.2f\", $total_bytes / 1073741824}")"
elif (( total_bytes >= 1048576 )); then
  printf "Total space %s: %.2f MB\n" "$label" "$(awk "BEGIN {printf \"%.2f\", $total_bytes / 1048576}")"
elif (( total_bytes >= 1024 )); then
  printf "Total space %s: %.2f KB\n" "$label" "$(awk "BEGIN {printf \"%.2f\", $total_bytes / 1024}")"
else
  printf "Total space %s: %d bytes\n" "$label" "$total_bytes"
fi
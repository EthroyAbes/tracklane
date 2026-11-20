"""
PriceIntel — publish.py

The four-phase publication pipeline. This is the only thing that should ever
replace the published report.

    1. Validate the collected data          (validation.py --phase data)
    2. Generate a CANDIDATE report          (generate_report.py --out <candidate>)
    3. Validate the candidate's contents    (validation.py --phase report)
    4. Promote the candidate to published   (atomic replace, previous kept)

Blocking integrity failures stop publication and preserve the previous report.
Since 2026-09-20, terminal partial collections may appear as explicitly disclosed
current observations. They remain excluded from complete-panel trend metrics;
candidate validation independently checks their coverage and captured prices.

Why this is not just a reordering of the scheduled tasks
--------------------------------------------------------
The obvious fix - run validation before generate_report and chain them with && -
does not work, for two independent reasons, both verified against the code:

  1. validation.py's run() printed findings and returned None. It exited 0 on
     every path, including the one where the database was missing entirely. `&&`
     would therefore have published through every failure mode, including
     "validation never actually ran". Fixed in validation.py: run() now returns
     an exit code and __main__ passes it to sys.exit().

  2. check_non_constituent_brand_published() reads the rendered report. Run
     before generation, it inspects YESTERDAY's file - so the reordering that was
     supposed to make the check gate today's output would instead have made it
     audit an artefact that was already published and pass today's through
     unexamined. Fixed by splitting validation into a data phase and a report
     phase, and pointing the report phase at the candidate.

Promotion is atomic (os.replace) so a reader can never fetch a half-written file.
The previous published report is copied aside first, so a bad promotion can be
undone without regenerating anything.

Publishing through the PythonAnywhere symlink
---------------------------------------------
On PythonAnywhere, `PythonAnywhere/report.html` inside the git clone is a SYMLINK
to `/home/Ethanabraham/report.html`. The web app's static mapping serves
`/report.html` from that absolute path, which has nothing to do with where the
git clone lives (see PRICEINTEL_PROJECT_SUMMARY.md).

A naive `os.replace(candidate, PythonAnywhere/report.html)` would therefore:

  1. replace the SYMLINK with a regular file inside the git clone,
  2. leave `/home/Ethanabraham/report.html` — the file actually served — stale
     forever, and
  3. destroy the bridge, so every subsequent publish would also go nowhere.

The site would keep serving the last good report indefinitely while this script
printed "PUBLISHED". That is precisely the class of silent failure this pipeline
exists to prevent, so promotion resolves the symlink first and replaces the
TARGET, never the link. The candidate is written into the target's own directory
so that `os.replace` is a same-filesystem rename and therefore atomic — across
filesystems it is not, and a reader could see a partial file.

After promotion the link is re-checked: if it is no longer a symlink, or no
longer points where it did, that is reported as a failure even though the bytes
were written, because the next publish would silently go to the wrong place.
"""

import argparse
import publication_record
import os
import shutil
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent
PUBLISHED_PATH = BASE_DIR / "PythonAnywhere" / "report.html"
GENERATOR = BASE_DIR / "PythonAnywhere" / "generate_report.py"
VALIDATOR = BASE_DIR / "validation.py"


def resolve_publish_target(published_path=None):
    """Where the bytes must actually land, and how they get there.

    Returns (target, via_symlink). `target` is the real file the web server
    reads: the symlink's destination when `published_path` is a symlink, and
    `published_path` itself otherwise. Only the final component is followed —
    resolve() would also collapse any symlinked parent directory, which would
    move the candidate out from under the link for no reason.
    """
    p = Path(published_path) if published_path else PUBLISHED_PATH
    if p.is_symlink():
        dest = Path(os.readlink(p))
        if not dest.is_absolute():
            dest = (p.parent / dest).resolve()
        return dest, True
    return p, False


def candidate_path_for(target):
    """Candidate lives beside the target so promotion is a same-filesystem
    rename. Writing it into the git clone and renaming across to
    /home/Ethanabraham/ would not be atomic."""
    return target.parent / "report.candidate.html"


def previous_path_for(target):
    return target.parent / "report.previous.html"

try:
    import notify
    NOTIFY_AVAILABLE = True
except ImportError:
    NOTIFY_AVAILABLE = False

EXIT_OK = 0
EXIT_DATA_VALIDATION_FAILED = 1
EXIT_GENERATION_FAILED = 2
EXIT_CANDIDATE_VALIDATION_FAILED = 3
EXIT_PROMOTION_FAILED = 4


class PhaseResult(int):
    """Exit-code compatible result retaining streamed diagnostic output."""
    def __new__(cls, code, output='', error=None, traceback_text=None):
        value = super().__new__(cls, code)
        value.output = output
        value.error = error
        value.traceback_text = traceback_text
        return value


def _run(cmd, label):
    print(f"\n{'=' * 70}\n{label}\n{'=' * 70}")
    print(f"$ {' '.join(str(c) for c in cmd)}\n")
    lines = []
    child_env = dict(os.environ, PYTHONIOENCODING='utf-8')
    # Retain exact Unicode for email; legacy Windows consoles may only render
    # escaped characters. Logging must never abort publication on encoding.
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(errors='backslashreplace')
    try:
        with subprocess.Popen([str(c) for c in cmd], cwd=str(BASE_DIR),
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding='utf-8', errors='replace', env=child_env) as proc:
            for line in proc.stdout:
                print(line, end='', flush=True)
                lines.append(line)
            code = proc.wait()
        print(f"\n-> {label}: exit {code}")
        return PhaseResult(code, ''.join(lines))
    except OSError as e:
        return PhaseResult(2, ''.join(lines), e, traceback.format_exc())


def _abort(stage, code, detail, target=None, phase_result=None, error=None, traceback_text=None):
    target = target or resolve_publish_target()[0]
    print(f"\n{'!' * 70}")
    print(f"PUBLICATION ABORTED at {stage}.")
    output = getattr(phase_result, 'output', '')
    if output:
        detail += "\n\nPhase output (including validation findings):\n" + output
    error = error or getattr(phase_result, 'error', None)
    traceback_text = traceback_text or getattr(phase_result, 'traceback_text', None)
    print(detail)
    if target.exists():
        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)
        freshness_detail = f"Report file: {target}; last written {mtime:%Y-%m-%d %H:%M:%S} UTC."
        print(freshness_detail)
    else:
        freshness_detail = f"No published report exists at {target}."
        print(freshness_detail)
    print("!" * 70)
    if NOTIFY_AVAILABLE:
        try:
            notify.send_failure(
                error, stage=f"publish.py: {stage}",
                details=detail + "\n\n" + freshness_detail,
                traceback_text=traceback_text, report_path=target,
            )
        except Exception as e:
            print(f"(failure notification did not send: {type(e).__name__}: {e})")
    return code


def publish(dry_run=False, published_path=None):
    python = sys.executable
    target, via_symlink = resolve_publish_target(published_path)
    candidate = candidate_path_for(target)
    previous = previous_path_for(target)
    link = Path(published_path) if published_path else PUBLISHED_PATH

    print(f"Publish target: {target}")
    if via_symlink:
        print(f"  (reached through the symlink {link} -> {target}; the symlink is "
              f"preserved and the target is replaced)")
    if not target.parent.exists():
        return _abort(
            "setup", EXIT_PROMOTION_FAILED,
            f"the publish target's directory {target.parent} does not exist. If "
            f"{link} is a symlink, it is dangling and must be repointed before "
            f"anything can be published.",
            target=target,
        )

    # ── Phase 1 — validate the collected data ────────────────────────────────
    code = _run([python, VALIDATOR, "--phase", "data", "--no-notify"],
                "PHASE 1 — validate collected data")
    if code != 0:
        return _abort(
            "phase 1 (data validation)", EXIT_DATA_VALIDATION_FAILED,
            f"validation.py --phase data exited {code}. "
            + ("A precondition failed - the checks could not run at all."
               if code == 2 else
               "Blocking findings were reported. Today's data is not publishable."),
            target=target, phase_result=code,
        )

    # ── Phase 2 — generate a candidate ───────────────────────────────────────
    if candidate.exists():
        candidate.unlink()  # never validate a stale candidate
    code = _run([python, GENERATOR, "--out", candidate],
                "PHASE 2 — generate candidate report")
    if code != 0 or not candidate.exists():
        return _abort(
            "phase 2 (generation)", EXIT_GENERATION_FAILED,
            f"generate_report.py exited {code} "
            f"(candidate {'written' if candidate.exists() else 'NOT written'}).",
            target=target, phase_result=code,
        )
    print(f"   candidate: {candidate} ({candidate.stat().st_size:,} bytes)")

    # ── Phase 3 — validate the candidate's contents ──────────────────────────
    code = _run([python, VALIDATOR, "--phase", "report",
                 "--report-path", candidate, "--no-notify"],
                "PHASE 3 — validate candidate contents")
    if code != 0:
        return _abort(
            "phase 3 (candidate validation)", EXIT_CANDIDATE_VALIDATION_FAILED,
            f"validation.py --phase report exited {code} against the candidate. "
            f"The candidate is left at {candidate} for inspection and has NOT "
            f"been published.",
            target=target, phase_result=code,
        )

    # ── Phase 4 — promote ────────────────────────────────────────────────────
    if dry_run:
        print("\nDRY RUN — all phases passed; promotion skipped. "
              f"Candidate left at {candidate}.")
        return EXIT_OK

    print(f"\n{'=' * 70}\nPHASE 4 — promote candidate to published\n{'=' * 70}")
    link_was_symlink = link.is_symlink()
    link_pointed_at = os.readlink(link) if link_was_symlink else None
    try:
        if target.exists():
            shutil.copy2(target, previous)
            print(f"   previous approved report kept at {previous}")
        # Same-directory rename: atomic. A reader sees either the old file or the
        # new one, never a partially written one. Replaces the TARGET, so a
        # symlink pointing here keeps pointing here.
        os.replace(candidate, target)
        print(f"   published -> {target}")
    except OSError as e:
        return _abort("phase 4 (promotion)", EXIT_PROMOTION_FAILED,
                      f"{type(e).__name__}: {e}", target=target,
                      error=e, traceback_text=traceback.format_exc())

    # The bytes landed. Now confirm the route to them still exists - a broken
    # link means the served page is stale and the NEXT publish would go nowhere,
    # which would otherwise not surface until someone noticed the report had
    # stopped changing.
    if link_was_symlink:
        if not link.is_symlink():
            return _abort(
                "phase 4 (post-promotion check)", EXIT_PROMOTION_FAILED,
                f"{link} was a symlink before promotion and is not one now. The "
                f"bytes were written to {target}, but the link the deployment "
                f"depends on has been destroyed and must be recreated:\n"
                f"    ln -sf {target} {link}",
                target=target,
            )
        now_points_at = os.readlink(link)
        if now_points_at != link_pointed_at:
            return _abort(
                "phase 4 (post-promotion check)", EXIT_PROMOTION_FAILED,
                f"{link} now points at {now_points_at}, not {link_pointed_at}.",
                target=target,
            )
        print(f"   symlink intact: {link} -> {now_points_at}")

    try:
        publication_record.record_success(target)
    except OSError as e:
        # The report already passed and was promoted. Do not pretend promotion
        # failed or infer freshness from collection; missing receipt is unknown.
        print(f"WARNING: report published but publication record could not be saved: {e}")
    else:
        try:
            import publication_notification
            publication_notification.send(target)
        except Exception as e:
            # A transport failure cannot undo a successfully promoted report.
            print(f"WARNING: report published but confirmation email failed: {e}")
    print("\nPUBLISHED. All four phases passed.")
    return EXIT_OK


def main():
    parser = argparse.ArgumentParser(
        description="PriceIntel — validated publication pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run all four phases but do not replace the published "
                             "report. Use this for the first live run.")
    parser.add_argument("--published-path", default=None,
                        help="Override the published report path. For testing "
                             "against a replica of the deployment layout; not for "
                             "normal use.")
    # There is deliberately no flag to skip validation. An earlier version had
    # --skip-data-validation "to re-render an already-validated day", which in
    # practice is an unrestricted bypass of the only gate on publication, on a
    # pipeline whose entire purpose is that the gate cannot be bypassed. It would
    # have been reached for on exactly the mornings when something looked wrong
    # and the report was wanted anyway. Re-rendering a good day costs one more
    # validation pass, which is cheap; a bypass costs the guarantee.
    args = parser.parse_args()
    return publish(dry_run=args.dry_run, published_path=args.published_path)


if __name__ == "__main__":
    sys.exit(main())

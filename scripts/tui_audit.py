"""TUI audit script - headless test of all screens with screenshots.

Usage: uv run python scripts/tui_audit.py /path/to/pipeline/output
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

SCREENS = ["home", "entropy", "contracts", "actions", "query"]
SCREENSHOT_DIR = Path("tui_screenshots")


async def audit_screen(output_dir: Path, screen_name: str) -> tuple[str, bool, str]:
    """Test a single screen headlessly and take a screenshot.

    Returns: (screen_name, success, message)
    """
    from dataraum.cli.tui.app import DataraumApp

    app = DataraumApp(output_dir=output_dir, initial_screen=screen_name)

    try:
        async with app.run_test(size=(120, 40)) as pilot:
            # Wait for screen to fully render
            await pilot.pause()
            await pilot.pause()
            await pilot.pause()

            # Take screenshot
            SCREENSHOT_DIR.mkdir(exist_ok=True)
            svg_path = SCREENSHOT_DIR / f"{screen_name}.svg"
            app.save_screenshot(str(svg_path))

            return (screen_name, True, f"OK - screenshot saved to {svg_path}")
    except Exception as e:
        return (screen_name, False, f"CRASH: {type(e).__name__}: {e}")


async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: uv run python scripts/tui_audit.py /path/to/pipeline/output")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    if not (output_dir / "metadata.db").exists():
        print(f"Error: No metadata.db in {output_dir}")
        sys.exit(1)

    print(f"TUI Audit - testing {len(SCREENS)} screens against {output_dir}\n")

    results = []
    for screen in SCREENS:
        print(f"  Testing {screen}...", end=" ", flush=True)
        name, ok, msg = await audit_screen(output_dir, screen)
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {msg}")
        results.append((name, ok, msg))

    print(f"\n{'=' * 60}")
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"Result: {passed}/{len(results)} screens passed")

    if any(not ok for _, ok, _ in results):
        print("\nFailed screens:")
        for name, ok, msg in results:
            if not ok:
                print(f"  - {name}: {msg}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

"""
Generate a markdown quality report from the Silver quality_summary Delta table.

Reads per-entity, per-check pass/fail metrics and writes quality-report.md
showing pass percentages across all four Silver quality check categories.

Prerequisite: Run src/silver/transform_all.py first.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from silver_common import get_spark, resolve_layer_paths

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CHECK_CATEGORIES = [
    "Completeness",
    "Uniqueness",
    "Referential Integrity",
    "Logic & Type",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_quality_summary(spark: SparkSession):
    path = resolve_layer_paths("quality_summary", "")
    logger.info("Reading quality summary from: %s", path)
    return spark.read.format("delta").load(path)


def _build_report_markdown(rows: list[dict]) -> str:
    lines = [
        "# Silver Data Quality Report",
        "",
        f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "",
        "## Summary by Entity and Check Category",
        "",
        "| Entity | Check Category | Total Rows | Passed | Failed | Pass % |",
        "|--------|----------------|------------|--------|--------|--------|",
    ]

    for row in rows:
        lines.append(
            f"| {row['entity']} | {row['check_category']} | "
            f"{int(row['total_rows'])} | {int(row['passed_count'])} | "
            f"{int(row['failed_count'])} | {row['pass_percentage']:.2f}% |"
        )

    lines.extend(["", "## Aggregate Pass Rate by Check Category", ""])

    category_totals: dict[str, dict[str, float]] = {}
    for row in rows:
        cat = row["check_category"]
        if cat not in category_totals:
            category_totals[cat] = {"passed": 0.0, "total": 0.0}
        category_totals[cat]["passed"] += row["passed_count"]
        category_totals[cat]["total"] += row["total_rows"]

    lines.append("| Check Category | Total Rows | Passed | Failed | Pass % |")
    lines.append("|----------------|------------|--------|--------|--------|")
    for cat in CHECK_CATEGORIES:
        if cat in category_totals:
            totals = category_totals[cat]
            passed = totals["passed"]
            total = totals["total"]
            failed = total - passed
            pct = round((passed / total) * 100, 2) if total > 0 else 100.0
            lines.append(
                f"| {cat} | {int(total)} | {int(passed)} | {int(failed)} | {pct}% |"
            )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Metrics are computed at the Silver layer; invalid rows are flagged, not deleted.",
            "- Each entity is evaluated independently across all four check categories.",
            "- Target: 700 intentional defects injected during data generation.",
            "- See `data/manifest/defect_manifest.csv` for the full defect registry.",
            "",
        ]
    )

    return "\n".join(lines)


def generate_quality_report(output_path: Path | None = None) -> Path:
    spark = get_spark()
    summary_df = _read_quality_summary(spark)

    rows = [row.asDict() for row in summary_df.collect()]
    rows.sort(key=lambda r: (r["entity"], r["check_category"]))

    markdown = _build_report_markdown(rows)

    if output_path is None:
        output_path = _project_root() / "quality-report.md"

    output_path.write_text(markdown, encoding="utf-8")
    logger.info("Quality report written: %s", output_path)

    print("\n" + "=" * 60)
    print("SILVER QUALITY REPORT")
    print("=" * 60)
    for row in rows:
        print(
            f"  {row['entity']:12} | {row['check_category']:22} | "
            f"pass={row['pass_percentage']:.2f}%"
        )
    print("=" * 60)

    return output_path


def main() -> None:
    generate_quality_report()


if __name__ == "__main__":
    main()

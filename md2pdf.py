#!/usr/bin/env python3
"""
Конвертер Markdown → PDF для документации проекта.

Преобразует .md в HTML (python-markdown с таблицами, подсветкой кода и
оглавлением), применяет аккуратную типографику и рендерит в PDF через WeasyPrint.

Зависимости:
    pip install markdown pygments      # Markdown → HTML
    WeasyPrint (HTML → PDF):
        macOS:  brew install weasyprint
        Ubuntu: sudo apt install weasyprint
        либо:   pip install weasyprint   (нужны системные libpango/cairo)

Использование:
    python3 md2pdf.py README.md USAGE.md          # рядом создаст README.pdf, USAGE.pdf
    python3 md2pdf.py README.md -o docs/readme.pdf
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import markdown
    from markdown.extensions.toc import slugify_unicode
except ImportError:
    sys.exit("Не установлен markdown. Установите: pip install markdown pygments")

# Типографика для PDF. Системные шрифты (есть кириллица), аккуратные таблицы и код.
CSS = """
@page { size: A4; margin: 18mm 16mm; }
body {
    font-family: -apple-system, "Segoe UI", "DejaVu Sans", Arial, sans-serif;
    font-size: 10.5pt; line-height: 1.5; color: #1a1a1a;
}
h1 { font-size: 20pt; border-bottom: 2px solid #2c3e50; padding-bottom: 4px; margin-top: 0; }
h2 { font-size: 15pt; border-bottom: 1px solid #ddd; padding-bottom: 3px; margin-top: 22px; }
h3 { font-size: 12.5pt; margin-top: 16px; }
h1, h2, h3, h4 { color: #2c3e50; page-break-after: avoid; }
p, li { orphans: 2; widows: 2; }
a { color: #2563eb; text-decoration: none; }
code {
    font-family: "SF Mono", "DejaVu Sans Mono", Menlo, Consolas, monospace;
    font-size: 9pt; background: #f3f4f6; padding: 1px 4px; border-radius: 3px;
}
pre {
    background: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 6px;
    padding: 10px 12px; page-break-inside: avoid; white-space: pre-wrap; word-wrap: break-word;
}
pre code { background: none; padding: 0; font-size: 8.6pt; line-height: 1.4; }
table {
    border-collapse: collapse; width: 100%; margin: 12px 0;
    font-size: 9.2pt; page-break-inside: avoid;
}
th, td { border: 1px solid #d0d7de; padding: 5px 8px; text-align: left; vertical-align: top; }
th { background: #f0f3f6; font-weight: 600; }
tr:nth-child(even) td { background: #fafbfc; }
blockquote {
    margin: 10px 0; padding: 6px 14px; border-left: 4px solid #d0d7de;
    background: #f8f9fa; color: #444;
}
hr { border: none; border-top: 1px solid #ddd; margin: 18px 0; }
.codehilite { background: #f6f8fa; }
img { max-width: 100%; }
"""

HTML_TMPL = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><title>{title}</title>
<style>{css}</style></head><body>{body}</body></html>"""


def md_to_html(md_text: str) -> str:
    return markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "codehilite", "toc", "sane_lists", "admonition"],
        extension_configs={
            "codehilite": {"guess_lang": False, "noclasses": True},
            # slugify_unicode сохраняет кириллицу в id заголовков — иначе якоря
            # оглавления (#1-назначение и т.п.) не совпадают со ссылками.
            "toc": {"slugify": slugify_unicode},
        },
    )


def find_weasyprint() -> list[str] | None:
    """Найти способ запустить WeasyPrint: CLI или модуль python."""
    exe = shutil.which("weasyprint")
    if exe:
        return [exe]
    # Через интерпретатор текущего venv, если установлен как пакет.
    try:
        import weasyprint  # noqa: F401
        return [sys.executable, "-m", "weasyprint"]
    except ImportError:
        return None


def convert(md_path: Path, pdf_path: Path, runner: list[str]) -> None:
    html = HTML_TMPL.format(
        title=md_path.stem,
        css=CSS,
        body=md_to_html(md_path.read_text(encoding="utf-8")),
    )
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(html)
        tmp_html = f.name
    try:
        # base_url = папка исходника, чтобы относительные ссылки/картинки работали.
        subprocess.run(
            runner + [tmp_html, str(pdf_path), "--base-url", str(md_path.resolve().parent)],
            check=True,
        )
    finally:
        Path(tmp_html).unlink(missing_ok=True)
    print(f"  {md_path.name} → {pdf_path}")


def main():
    p = argparse.ArgumentParser(
        description="Конвертер Markdown → PDF.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("files", nargs="+", help="Markdown-файлы (.md)")
    p.add_argument("-o", "--output", default=None,
                   help="Имя выходного PDF (только при одном входном файле)")
    args = p.parse_args()

    runner = find_weasyprint()
    if runner is None:
        sys.exit("Не найден WeasyPrint. Установите: brew install weasyprint "
                 "(macOS) / apt install weasyprint (Ubuntu) / pip install weasyprint")

    if args.output and len(args.files) > 1:
        sys.exit("--output можно указывать только для одного входного файла")

    for f in args.files:
        md_path = Path(f)
        if not md_path.is_file():
            print(f"Пропуск: файл не найден — {f}", file=sys.stderr)
            continue
        pdf_path = Path(args.output) if args.output else md_path.with_suffix(".pdf")
        convert(md_path, pdf_path, runner)


if __name__ == "__main__":
    main()

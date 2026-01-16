from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from fpdf import FPDF


_CYRILLIC_MAP = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "yo",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "kh",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


def _transliterate(source: str) -> str:
    result: List[str] = []
    for ch in source or "":
        lower = ch.lower()
        if lower in _CYRILLIC_MAP:
            repl = _CYRILLIC_MAP[lower]
            if ch.isupper():
                repl = repl.capitalize()
            result.append(repl)
            continue
        if ch in {"«", "»"}:
            result.append('"')
            continue
        if ch == "№":
            result.append("No")
            continue
        code = ord(ch)
        if 32 <= code < 127:
            result.append(ch)
        else:
            result.append("?")
    return "".join(result)


def _format_datetime(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return str(value)


def build_payments_pdf(payments: List[Dict[str, Any]], destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    header = f"Otcet po oplatam - {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}"
    pdf.cell(0, 10, header, ln=1)
    pdf.ln(3)
    pdf.set_font("Helvetica", size=10)
    if not payments:
        pdf.multi_cell(0, 6, "Net dannyh dlya otcheta.")
    status_map = {
        "approved": "Oplachen",
        "pending": "Ozhidaet",
        "declined": "Otkazano",
    }
    for payment in payments:
        username = payment.get("username")
        username_part = f"@{username}" if username else "-"
        user_line = (
            f"Polzovatel: {payment.get('full_name') or '-'} "
            f"(ID {payment.get('user_id')}) {username_part}"
        )
        lines = [
            f"Zayavka: {payment.get('request_id')}",
            user_line,
            f"Karta: {payment.get('card_number') or '-'} / {payment.get('card_name') or '-'}",
            f"Status: {status_map.get(payment.get('status'), payment.get('status', '-'))}",
            f"Sozdano: {_format_datetime(payment.get('created_at'))}",
            f"Reshenie: {_format_datetime(payment.get('resolved_at'))}",
        ]
        for line in lines:
            pdf.multi_cell(0, 6, _transliterate(line))
        pdf.ln(2)
    pdf.output(str(destination))
    return destination

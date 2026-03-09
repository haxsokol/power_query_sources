from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print(
        "Не найден polars. Создайте окружение через './setup.sh' и запустите скрипт из venv.",
        file=sys.stderr,
    )
    raise SystemExit(1)


KNOWN_FUNCTIONS = [
    "Value.NativeQuery",
    "PostgreSQL.Database",
    "Oracle.Database",
    "SapHana.Database",
    "Sql.Database",
    "MySQL.Database",
    "Teradata.Database",
    "SapBusinessWarehouse.Cubes",
    "AnalysisServices.Database",
    "GoogleBigQuery.Database",
    "Snowflake.Databases",
    "Excel.Workbook",
    "Excel.CurrentWorkbook",
    "Csv.Document",
    "File.Contents",
    "Folder.Files",
    "Folder.Contents",
    "SharePoint.Files",
    "SharePoint.Contents",
    "Web.Contents",
    "Odbc.Query",
    "Odbc.DataSource",
    "OleDb.DataSource",
    "Access.Database",
]

SOURCE_TYPE_NAMES = {
    "Value.NativeQuery": "NativeQuery",
    "PostgreSQL.Database": "PostgreSQL",
    "Oracle.Database": "Oracle",
    "SapHana.Database": "SAP HANA",
    "Sql.Database": "SQL Server",
    "MySQL.Database": "MySQL",
    "Teradata.Database": "Teradata",
    "SapBusinessWarehouse.Cubes": "SAP BW",
    "AnalysisServices.Database": "Analysis Services",
    "GoogleBigQuery.Database": "BigQuery",
    "Snowflake.Databases": "Snowflake",
    "Excel.Workbook": "Excel",
    "Excel.CurrentWorkbook": "Excel.CurrentWorkbook",
    "Csv.Document": "CSV",
    "File.Contents": "File",
    "Folder.Files": "Folder",
    "Folder.Contents": "Folder",
    "SharePoint.Files": "SharePoint",
    "SharePoint.Contents": "SharePoint",
    "Web.Contents": "Web",
    "Odbc.Query": "ODBC",
    "Odbc.DataSource": "ODBC",
    "OleDb.DataSource": "OleDb",
    "Access.Database": "Access",
}

COLUMNS = [
    "PowerQuery",
    "Группа",
    "Источник",
    "ИмяБД",
    "ТаблицаВБД",
    "Сервер",
    "ПутьКФайлу",
    "Объект",
]

FUNCTION_RE = re.compile(
    r"(?<![A-Za-z0-9_])("
    + "|".join(re.escape(name) for name in KNOWN_FUNCTIONS)
    + r")\s*\(",
    re.IGNORECASE,
)
PARTITION_RE = re.compile(r"(?im)^\s*partition\s+(?P<name>.+?)\s*=\s*m\s*$")
QUERY_GROUP_RE = re.compile(r"(?im)^\s*queryGroup:\s*(?P<value>.+?)\s*$")
SOURCE_RE = re.compile(r"(?im)^\s*source\s*=")
QUERY_OPTION_RE = re.compile(r'\bQuery\s*=\s*("(?:[^"]|"")*")', re.IGNORECASE)
NAVIGATION_RE = re.compile(
    r"\{\s*\[(?P<body>[^\]]*(?:\"(?:[^\"]|\"\")*\"[^\]]*)*)\]\s*\}\s*\[Data\]",
    re.IGNORECASE,
)
KEY_VALUE_RE = re.compile(r'(\w+)\s*=\s*("(?:[^"]|"")*")', re.IGNORECASE)
CONNECTION_RE = re.compile(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$")
SQL_OBJECT_RE = re.compile(
    r"""
    (?is)
    \b(?:from|join|update|into|merge\s+into)\b
    \s+
    (?P<object>
        (?:"(?:[^"]|"")+"|\[[^\]]+\]|`[^`]+`|[A-Za-z_][\w$#@]*)
        (?:\s*\.\s*(?:"(?:[^"]|"")+"|\[[^\]]+\]|`[^`]+`|[A-Za-z_][\w$#@]*))*
    )
    """,
    re.VERBOSE,
)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[7:].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Извлекает источники Power Query из .tmdl и сохраняет результаты в Excel."
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default=None,
        help="Путь к .tmdl файлу или директории с .tmdl. Если не задан, берется PQS_INPUT_PATH или toml_files рядом со скриптом.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Папка для выходных .xlsx. Если не задано, берется PQS_OUTPUT_DIR или find_source_excel рядом со скриптом.",
    )
    parser.add_argument(
        "--glob",
        default=None,
        help="Маска поиска .tmdl в директории. Если не задано, берется PQS_GLOB или *.tmdl",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Путь к .env файлу (относительные пути считаются от директории скрипта).",
    )
    return parser.parse_args()


def resolve_config_path(raw_value: str, script_dir: Path, from_cli: bool) -> Path:
    path = Path(raw_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if from_cli:
        return path.resolve()
    return (script_dir / path).resolve()


def read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "utf-16", "cp1251"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeError:
            continue
    raise UnicodeError(f"Не удалось прочитать файл: {path}")


def normalize_whitespace(value: str) -> str:
    return re.sub(
        r"\s+", " ", value.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    ).strip()


def decode_m_string(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        value = value[1:-1].replace('""', '"')
    return value.replace("#(lf)", "\n").replace("#(cr)", "\r").replace("#(tab)", "\t")


def normalize_sql(value: str) -> str:
    return normalize_whitespace(decode_m_string(value))


def find_matching_paren(text: str, open_index: int) -> int:
    depth = 0
    in_string = False
    i = open_index
    while i < len(text):
        char = text[i]
        if char == '"':
            if in_string and i + 1 < len(text) and text[i + 1] == '"':
                i += 2
                continue
            in_string = not in_string
        elif not in_string:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1


def split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    round_depth = square_depth = curly_depth = 0
    in_string = False
    i = 0

    while i < len(text):
        char = text[i]
        if char == '"':
            if in_string and i + 1 < len(text) and text[i + 1] == '"':
                i += 2
                continue
            in_string = not in_string
        elif not in_string:
            if char == "(":
                round_depth += 1
            elif char == ")":
                round_depth -= 1
            elif char == "[":
                square_depth += 1
            elif char == "]":
                square_depth -= 1
            elif char == "{":
                curly_depth += 1
            elif char == "}":
                curly_depth -= 1
            elif char == "," and round_depth == square_depth == curly_depth == 0:
                parts.append(text[start:i].strip())
                start = i + 1
        i += 1

    last = text[start:].strip()
    if last:
        parts.append(last)
    return parts


def extract_call(
    text: str, start_index: int, function_name: str
) -> tuple[str, str, int] | None:
    open_index = text.find("(", start_index + len(function_name))
    if open_index == -1:
        return None
    close_index = find_matching_paren(text, open_index)
    if close_index == -1:
        return None
    return (
        text[start_index : close_index + 1],
        text[open_index + 1 : close_index],
        close_index + 1,
    )


def strip_identifier_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1].replace('""', '"')
    if len(value) >= 2 and value[0] == "[" and value[-1] == "]":
        return value[1:-1]
    if len(value) >= 2 and value[0] == "`" and value[-1] == "`":
        return value[1:-1]
    return value


def clean_sql_object(value: str) -> str:
    parts = re.findall(r'"(?:[^"]|"")+"|\[[^\]]+\]|`[^`]+`|[A-Za-z_][\w$#@]*', value)
    return ".".join(strip_identifier_quotes(part) for part in parts)


def extract_sql_tables(query_text: str) -> list[str]:
    tables: list[str] = []
    seen: set[str] = set()
    for match in SQL_OBJECT_RE.finditer(query_text or ""):
        table_name = clean_sql_object(match.group("object"))
        if table_name and table_name not in seen:
            seen.add(table_name)
            tables.append(table_name)
    return tables


def extract_query_option(arguments_text: str) -> str:
    match = QUERY_OPTION_RE.search(arguments_text)
    return normalize_sql(match.group(1)) if match else ""


def extract_navigation_object(block_text: str) -> str:
    found: list[str] = []
    for match in NAVIGATION_RE.finditer(block_text):
        values = {
            k.lower(): decode_m_string(v)
            for k, v in KEY_VALUE_RE.findall(match.group("body"))
        }
        name = values.get("item") or values.get("name")
        schema = values.get("schema")
        kind = values.get("kind")
        if not name:
            continue
        if schema:
            found.append(f"{schema}.{name}")
        elif kind:
            found.append(f"{name} ({kind})")
        else:
            found.append(name)
    return found[-1] if found else ""


def extract_nested_literal(arguments_text: str, function_name: str) -> str:
    match = re.search(
        rf"(?<![A-Za-z0-9_]){re.escape(function_name)}\s*\(",
        arguments_text,
        re.IGNORECASE,
    )
    if not match:
        return ""
    call = extract_call(arguments_text, match.start(), function_name)
    if not call:
        return ""
    _, nested_args, _ = call
    parts = split_top_level(nested_args)
    return decode_m_string(parts[0]) if parts else ""


def parse_connection_string(connection_string: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in connection_string.split(";"):
        if not part.strip():
            continue
        match = CONNECTION_RE.match(part)
        if match:
            result[match.group(1).strip().lower()] = match.group(2).strip()
    return result


def base_info(function_name: str) -> dict[str, str | list[str]]:
    return {
        "source_type": SOURCE_TYPE_NAMES.get(function_name, function_name),
        "db_name": "",
        "server": "",
        "file_path": "",
        "object_name": "",
        "tables": [],
    }


def parse_connector(
    function_name: str, arguments_text: str, block_text: str
) -> dict[str, str | list[str]]:
    info = base_info(function_name)
    parts = split_top_level(arguments_text)
    object_name = extract_navigation_object(block_text)
    query_text = extract_query_option(arguments_text)
    tables = extract_sql_tables(query_text)

    lower_name = function_name.lower()
    if lower_name in {
        "postgresql.database",
        "sql.database",
        "mysql.database",
        "analysisservices.database",
    }:
        info["server"] = decode_m_string(parts[0]) if len(parts) > 0 else ""
        info["db_name"] = decode_m_string(parts[1]) if len(parts) > 1 else ""
    elif lower_name in {"oracle.database", "saphana.database", "teradata.database"}:
        info["db_name"] = decode_m_string(parts[0]) if parts else ""
        info["server"] = info["db_name"]
    elif lower_name == "snowflake.databases":
        info["server"] = decode_m_string(parts[0]) if parts else ""
    elif lower_name == "googlebigquery.database":
        info["db_name"] = decode_m_string(parts[0]) if parts else ""
    elif lower_name == "sapbusinesswarehouse.cubes":
        info["server"] = decode_m_string(parts[0]) if parts else ""
    elif lower_name == "odbc.query":
        props = parse_connection_string(decode_m_string(parts[0]) if parts else "")
        info["server"] = (
            props.get("server")
            or props.get("host")
            or props.get("data source")
            or props.get("dsn", "")
        )
        info["db_name"] = props.get("database") or props.get("initial catalog", "")
        if len(parts) > 1:
            tables = extract_sql_tables(normalize_sql(parts[1]))
    elif lower_name == "odbc.datasource":
        props = parse_connection_string(decode_m_string(parts[0]) if parts else "")
        info["server"] = (
            props.get("server")
            or props.get("host")
            or props.get("data source")
            or props.get("dsn", "")
        )
        info["db_name"] = props.get("database") or props.get("initial catalog", "")
    elif lower_name == "oledb.datasource":
        props = parse_connection_string(decode_m_string(parts[0]) if parts else "")
        info["server"] = props.get("data source", "")
        info["db_name"] = props.get("initial catalog", "") or props.get("database", "")
    elif lower_name in {"excel.workbook", "csv.document"}:
        info["file_path"] = extract_nested_literal(
            arguments_text, "File.Contents"
        ) or extract_nested_literal(arguments_text, "Web.Contents")
    elif lower_name == "file.contents":
        info["file_path"] = decode_m_string(parts[0]) if parts else ""
    elif lower_name in {
        "folder.files",
        "folder.contents",
        "sharepoint.files",
        "sharepoint.contents",
        "web.contents",
    }:
        info["file_path"] = decode_m_string(parts[0]) if parts else ""
    elif lower_name == "access.database":
        info["file_path"] = decode_m_string(parts[0]) if parts else ""
        info["db_name"] = Path(str(info["file_path"])).name if info["file_path"] else ""

    info["object_name"] = object_name
    info["tables"] = tables or ([object_name] if object_name else [])
    return info


def parse_source_call(
    function_name: str, arguments_text: str, block_text: str
) -> dict[str, str | list[str]]:
    if function_name.lower() != "value.nativequery":
        return parse_connector(function_name, arguments_text, block_text)

    parts = split_top_level(arguments_text)
    query_text = normalize_sql(parts[1]) if len(parts) > 1 else ""
    tables = extract_sql_tables(query_text)
    object_name = extract_navigation_object(block_text)

    if parts:
        connector_match = re.match(r"\s*([A-Za-z][A-Za-z0-9_.]*)\s*\(", parts[0])
        if connector_match:
            nested_name = connector_match.group(1)
            nested_call = extract_call(parts[0], connector_match.start(1), nested_name)
            info = parse_connector(
                nested_name, nested_call[1] if nested_call else parts[0], block_text
            )
        else:
            info = base_info(function_name)
    else:
        info = base_info(function_name)

    info["tables"] = tables or info.get("tables", [])
    if object_name and not info["tables"]:
        info["tables"] = [object_name]
    return info


def iter_partition_blocks(text: str):
    matches = list(PARTITION_RE.finditer(text))
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block_text = text[start:end]
        source_match = SOURCE_RE.search(block_text)
        if not source_match:
            continue
        query_group_match = QUERY_GROUP_RE.search(block_text)
        yield {
            "power_query": match.group("name").strip(),
            "query_group": (
                query_group_match.group("value").strip() if query_group_match else ""
            ),
            "source_text": block_text[source_match.start() :],
        }


def collect_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()

    for partition in iter_partition_blocks(text):
        source_text = partition["source_text"]
        skip_until = 0

        for match in FUNCTION_RE.finditer(source_text):
            if match.start() < skip_until:
                continue

            function_name = match.group(1)
            call = extract_call(source_text, match.start(), function_name)
            if not call:
                continue

            _, arguments_text, end_index = call
            info = parse_source_call(function_name, arguments_text, source_text)
            tables = info.get("tables") or [""]

            for table_name in tables:
                row = {
                    "PowerQuery": str(partition["power_query"]),
                    "Группа": str(partition["query_group"]),
                    "Источник": str(info.get("source_type", "")),
                    "ИмяБД": str(info.get("db_name", "")),
                    "ТаблицаВБД": str(table_name),
                    "Сервер": str(info.get("server", "")),
                    "ПутьКФайлу": str(info.get("file_path", "")),
                    "Объект": str(info.get("object_name", "")),
                }
                key = tuple(row[column] for column in COLUMNS)
                if key not in seen:
                    seen.add(key)
                    rows.append(row)

            skip_until = end_index

    return rows


def build_dataframe(rows: list[dict[str, str]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame({column: [] for column in COLUMNS})
    return pl.DataFrame(rows).select(COLUMNS)


def normalize_table_key(value: str) -> str:
    return normalize_whitespace(value).casefold()


def deduplicate_by_table(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    unique_rows: list[dict[str, str]] = []
    seen_tables: set[str] = set()

    for row in rows:
        table_key = normalize_table_key(str(row.get("ТаблицаВБД", "")))
        if table_key:
            if table_key in seen_tables:
                continue
            seen_tables.add(table_key)
        unique_rows.append(row)

    return unique_rows


def discover_tmdl_files(input_path: Path, pattern: str) -> list[Path]:
    if not input_path.exists():
        raise FileNotFoundError(f"Путь не найден: {input_path}")

    if input_path.is_file():
        if input_path.suffix.lower() != ".tmdl":
            raise ValueError(f"Ожидался .tmdl файл, получено: {input_path}")
        return [input_path]

    return sorted(
        path.resolve()
        for path in input_path.rglob(pattern)
        if path.is_file() and path.suffix.lower() == ".tmdl"
    )


def reserve_output_path(base_path: Path, occupied: set[Path]) -> Path:
    if base_path not in occupied:
        occupied.add(base_path)
        return base_path

    index = 2
    while True:
        candidate = base_path.with_name(f"{base_path.stem}_{index}{base_path.suffix}")
        if candidate not in occupied:
            occupied.add(candidate)
            return candidate
        index += 1


def resolve_output_path(
    input_path: Path, output_dir: Path | None, occupied: set[Path]
) -> Path:
    target_dir = output_dir or input_path.parent
    base_path = (target_dir / f"{input_path.stem}_источники.xlsx").resolve()
    return reserve_output_path(base_path, occupied)


def write_excel(dataframe: pl.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        dataframe.write_excel(
            output_path,
            worksheet="Источники",
            autofilter=True,
            autofit=True,
            table_style="Table Style Medium 15",
        )
        return output_path
    except PermissionError:
        fallback = output_path.with_name(
            f"{output_path.stem}_{datetime.now():%Y%m%d_%H%M%S}{output_path.suffix}"
        )
        dataframe.write_excel(
            fallback,
            worksheet="Источники",
            autofilter=True,
            autofit=True,
            table_style="Table Style Medium 15",
        )
        print(
            f"Основной файл занят, поэтому Excel сохранен в новый файл: {fallback}",
            file=sys.stderr,
        )
        return fallback


def main() -> int:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    env_file = Path(args.env_file).expanduser()
    if not env_file.is_absolute():
        env_file = script_dir / env_file
    load_env_file(env_file)

    input_from_cli = args.input_path is not None
    output_from_cli = args.output_dir is not None

    input_value = args.input_path if input_from_cli else (os.getenv("PQS_INPUT_PATH") or "toml_files")
    output_value = args.output_dir if output_from_cli else (os.getenv("PQS_OUTPUT_DIR") or "find_source_excel")
    glob_pattern = args.glob or os.getenv("PQS_GLOB") or "*.tmdl"

    input_path = resolve_config_path(input_value, script_dir, input_from_cli)
    output_dir = resolve_config_path(output_value, script_dir, output_from_cli)

    try:
        input_files = discover_tmdl_files(input_path, glob_pattern)
    except (FileNotFoundError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1

    if not input_files:
        print(f"Не найдено .tmdl файлов по пути: {input_path}", file=sys.stderr)
        return 1

    occupied_outputs: set[Path] = set()
    success_count = 0
    total_rows = 0

    for toml_path in input_files:
        try:
            text = read_text(toml_path)
        except UnicodeError as error:
            print(f"[ERROR] {toml_path}: {error}", file=sys.stderr)
            continue

        rows = deduplicate_by_table(collect_rows(text))
        dataframe = build_dataframe(rows)
        output_path = resolve_output_path(toml_path, output_dir, occupied_outputs)
        actual_output_path = write_excel(dataframe, output_path)

        success_count += 1
        total_rows += dataframe.height
        print(f"[OK] {toml_path}")
        print(f"  Строк найдено: {dataframe.height}")
        print(f"  Excel: {actual_output_path}")

    if success_count == 0:
        print("Ни один .tmdl файл не удалось обработать.", file=sys.stderr)
        return 1

    print(f"Обработано TMDL: {success_count} из {len(input_files)}")
    print(f"Всего строк в Excel: {total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

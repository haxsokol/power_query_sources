# power_query_sources

Скрипт извлекает источники Power Query из TMDL-файлов и формирует Excel-отчет.

## Структура после setup

После запуска `setup.sh` или `setup.ps1` автоматически создаются папки:

- `tmdl_files` — сюда кладем `.tmdl`
- `find_source_excel` — сюда складываются итоговые `.xlsx`

## 1. Подготовка окружения

Инструкция предполагает, что терминал уже открыт в папке `power_query_sources`.

### Linux/macOS или Git Bash

```bash
chmod +x setup.sh
./setup.sh
```

### Windows (PowerShell)

```powershell
.\setup.ps1
```

Если PowerShell блокирует запуск скриптов:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\setup.ps1
```

## 2. Куда класть TMDL и где смотреть Excel

1. Скопируйте `.tmdl` файлы в папку `tmdl_files`.
2. Запустите скрипт.
3. Готовые Excel-файлы будут в `find_source_excel`.

## 3. Запуск скрипта

Активируйте виртуальное окружение.

### Linux/macOS

```bash
source .venv/bin/activate
```

### Windows Git Bash

```bash
source .venv/Scripts/activate
```

### Windows (PowerShell)

```powershell
.\.venv\Scripts\Activate.ps1
```

Запуск по умолчанию (берет TMDL из `tmdl_files`, пишет Excel в `find_source_excel`):

```bash
python extract_power_query_sources.py
```

Дополнительно можно переопределить пути:

```bash
python extract_power_query_sources.py ../some_dir --output-dir ../another_dir
```

## 4. Особенности парсинга

- Имена запросов в колонке `PowerQuery` приводятся к корректным названиям из `annotation PBI_QueryOrder` (если аннотация есть в файле).
- Технические суффиксы вида `-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` и внешние кавычки у имен запросов автоматически убираются.
- Поддерживаются основные источники, включая `SharePoint.Tables`.
- Для SAP HANA-объектов вида `_SYS_BIC...bw2hana/...` в колонке `ИмяБД` автоматически проставляется `SAP HANA`.
- CTE-алиасы из SQL (`WITH ... AS (...)`) не попадают в `ТаблицаВБД`.
- `JOIN LATERAL` обрабатывается корректно: `LATERAL` не считается таблицей, но источники внутри lateral-подзапросов извлекаются.
- Источники извлекаются отдельно из `partition ... source = ...` и `expression ... = ...`, чтобы запросы не "подмешивались" друг в друга.

## 5. Переменные `.env`

Файл `.env` подхватывается автоматически. Текущие значения по умолчанию:

```dotenv
PQS_INPUT_PATH=tmdl_files
PQS_OUTPUT_DIR=find_source_excel
PQS_GLOB=*.tmdl
```

Для корпоративной сети можно задать fallback-индекс:

```dotenv
CORP_PIP_INDEX_URL=https://your.corp.pip/simple
```

Логика установки зависимостей в setup:

1. Сначала попытка установки из обычного PyPI.
2. Если не получилось — попытка через `CORP_PIP_INDEX_URL`.
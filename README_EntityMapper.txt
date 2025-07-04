# Snowflake Entity Mapper

The **Snowflake Entity Mapper** is a Python script designed to extract and consolidate **all columns** of entity-related data from two Snowflake tables:

- `PROD.PITCHBOOK.COMPANY_DATA_FEED`
- `PROD.VOLDEMORT.VOLDEMORT_FIRMOGRAPHICS`

It merges data based on entity IDs (Pitchbook and Voldemort IDs) provided in an Excel file, and outputs a unified CSV with prefixed column names for easy traceability.

---

## 📦 Features

- Connects to Snowflake securely using user credentials.
- Retrieves all columns from both Pitchbook and Voldemort tables.
- Handles empty or malformed input gracefully.
- Adds `pb_` and `vd_` prefixes to distinguish column sources.
- Saves the final merged dataset to a timestamped CSV file.
- Logging to both console and file (`entity_mapper.log`) with optional verbose mode.

---

## 🛠️ Requirements

- Python 3.7+
- Snowflake Connector for Python
- pandas
- openpyxl (for Excel file handling)

Install required packages:

```bash
pip install snowflake-connector-python pandas openpyxl
```

---

## 🧪 Usage

```bash
python snowflake_entity_mapper.py \
  -i "input.xlsx" \
  -o "output.csv" \
  -a "YOUR_SNOWFLAKE_ACCOUNT" \
  -u "USERNAME" \
  -p "PASSWORD" \
  -w "WAREHOUSE_NAME" \
  -r "ROLE_NAME" \
  -v
```

### Arguments:

| Flag        | Description                                |
|-------------|--------------------------------------------|
| `-i`        | Path to input Excel file (required)        |
| `-o`        | Path to output CSV file (optional)         |
| `-a`        | Snowflake account identifier (required)    |
| `-u`        | Snowflake username (required)              |
| `-p`        | Snowflake password (required)              |
| `-w`        | Warehouse name (default: `FORAGE_AI_WH`)   |
| `-r`        | Role name (default: `FORAGE_AI_USER`)      |
| `-v`        | Verbose logging mode (optional)            |

---

## 📥 Input Format

The input Excel file should have **at least two columns**:

1. Pitchbook Entity ID
2. Voldemort BQ ID

No header row is strictly required; the script reads the first two columns regardless of headers.

---

## 📤 Output

- A CSV file containing:
  - Original IDs
  - All Pitchbook fields (prefixed with `pb_`)
  - All Voldemort fields (prefixed with `vd_`)

Example:
```csv
pitchbook_id,bq_id,pb_COMPANY_NAME,...,vd_INDUSTRY,...
1023,874902,Acme Corp,...,Software,...
```

---

## 🔐 Security Note

⚠️ **Avoid committing sensitive credentials to version control.**

Use environment variables or secret managers in production instead of passing passwords via CLI.

---

## 📝 Example

```bash
python snowflake_entity_mapper.py \
  -i "test.xlsx" \
  -o "mapped1.csv" \
  -a "TWHRMQQ-EIA98922" \
  -u "JEHAN" \
  -p "qR9#kL8@xP2m" \
  -w "FORAGE_AI_WH" \
  -r "FORAGE_AI_USER" \
  -v
```

---

## 🧼 Logging

All logs are stored in:

```
entity_mapper.log
```

Use the `-v` flag to enable debug-level logging.

---

## 👨‍💻 Author

Developed by [Your Name].  
Contributions and issues welcome!

---

## 📄 License

This project is licensed under the MIT License.

# CSV Response Format

PgREST can return data in CSV format, which is useful for data export, spreadsheet applications, and bulk data processing.

## Requesting CSV Format

Use the `Accept` header to request CSV format:

```bash
curl -H "Accept: text/csv" http://localhost:3000/users
```

## Response Format

CSV responses include:
- Header row with column names
- Data rows with comma-separated values
- Proper escaping of special characters (commas, quotes, newlines)
- UTF-8 encoding

### Example Response

```http
HTTP/1.1 200 OK
Content-Type: text/csv; charset=utf-8

id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

## CSV Escaping

PgREST properly escapes CSV values:

- **Commas**: Values containing commas are quoted
- **Quotes**: Double quotes are escaped as `""`
- **Newlines**: Values containing newlines are quoted
- **Special characters**: Handled according to CSV standards

### Example with Special Characters

```bash
curl -H "Accept: text/csv" "http://localhost:3000/users?name=like.*Alice*"
```

Response:

```csv
id,name,email
1,"Alice, Smith","alice@example.com"
2,"Bob ""The Builder""","bob@example.com"
```

## Filtering and Selection

CSV format works with all query parameters:

```bash
# Filtered CSV
curl -H "Accept: text/csv" "http://localhost:3000/users?active=is.true"

# Selected columns
curl -H "Accept: text/csv" "http://localhost:3000/users?select=id,name,email"

# Ordered CSV
curl -H "Accept: text/csv" "http://localhost:3000/users?order=name.asc"
```

## Pagination

CSV responses support pagination:

```bash
# First 100 rows
curl -H "Accept: text/csv" "http://localhost:3000/users?limit=100"

# Using Range header
curl -H "Accept: text/csv" -H "Range: 0-99" http://localhost:3000/users
```

## Downloading CSV Files

### Using curl

Save CSV to file:

```bash
curl -H "Accept: text/csv" http://localhost:3000/users > users.csv
```

### Using Browser

Most browsers will download CSV files when accessed directly:

```javascript
const link = document.createElement('a');
link.href = 'http://localhost:3000/users';
link.setAttribute('download', 'users.csv');
link.setAttribute('Accept', 'text/csv');
link.click();
```

### Using JavaScript Fetch

```javascript
fetch('http://localhost:3000/users', {
  headers: {
    'Accept': 'text/csv',
    'Authorization': 'Bearer ' + token,
  },
})
  .then(response => response.text())
  .then(csv => {
    // Process CSV data
    console.log(csv);
    // Or download
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'users.csv';
    a.click();
  });
```

## Large Datasets

For large datasets, CSV format is efficient:

```bash
# Export all users
curl -H "Accept: text/csv" http://localhost:3000/users > all_users.csv

# Export with filters
curl -H "Accept: text/csv" \
  "http://localhost:3000/orders?created_at=gte.2024-01-01" \
  > orders_2024.csv
```

## Importing CSV

While PgREST doesn't directly import CSV, you can use PostgreSQL's `COPY` command:

```sql
-- Create temporary table
CREATE TEMP TABLE temp_users (id INT, name TEXT, email TEXT);

-- Copy CSV data
COPY temp_users FROM '/path/to/users.csv' WITH (FORMAT csv, HEADER true);

-- Insert into actual table
INSERT INTO users SELECT * FROM temp_users;
```

## Use Cases

1. **Data Export**: Export data for analysis in Excel, Google Sheets, etc.
2. **Reporting**: Generate CSV reports for business users
3. **Data Migration**: Export data for migration to other systems
4. **Bulk Processing**: Process large datasets in batch jobs
5. **Integration**: Integrate with systems that consume CSV

## Limitations

- **Embedding**: CSV format doesn't support resource embedding (use JSON for that)
- **Nested Data**: Complex nested JSON structures are flattened
- **Binary Data**: Binary columns are not included in CSV output

## Content-Type Header

CSV responses include the proper Content-Type header:

```
Content-Type: text/csv; charset=utf-8
```

This ensures browsers and tools recognize the format correctly.

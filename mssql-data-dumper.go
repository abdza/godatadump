package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"math"
  "strconv"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/xuri/excelize/v2"
)

var (
	verbose    bool
	maxWorkers int
	maxDBConns int
	batchSize  int
)

type DumpTask struct {
	DatadumpID    int
	TrackerModule string
	TrackerSlug   string
  TargetFile    string
  TableLimit    sql.NullInt64
	TableSort     sql.NullString
  TableCond     sql.NullString
}

func main() {
	startTime := time.Now()

	// Define command-line flags
	server := flag.String("server", "", "Database server address")
	port := flag.Int("port", 1433, "Database server port")
	user := flag.String("user", "", "Database username")
	password := flag.String("password", "", "Database password")
	database := flag.String("database", "", "Database name")
	datadumpID := flag.Int("datadump_id", 0, "Specific datadump ID to process (optional)")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose output")
	flag.IntVar(&maxWorkers, "max_workers", 5, "Maximum number of concurrent workers")
	flag.IntVar(&maxDBConns, "max_db_conns", 10, "Maximum number of database connections")
	flag.IntVar(&batchSize, "batch_size", 5000, "Number of rows to process in each batch")

	// Parse the flags
	flag.Parse()

	// Validate required flags
	if *server == "" || *user == "" || *password == "" || *database == "" {
		log.Fatal("All flags (server, user, password, database) are required")
	}

	fmt.Println("Starting MSSQL Data Dumper...")

	// Construct the connection string
	connString := fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s",
		*server, *port, *user, *password, *database)

	// Create a connection pool
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error creating database pool:", err)
	}
	defer db.Close()

	// Set maximum number of open connections
	db.SetMaxOpenConns(maxDBConns)
	db.SetMaxIdleConns(maxDBConns)

	fmt.Printf("Connected successfully. Max workers: %d, Max DB connections: %d, Batch size: %d\n", maxWorkers, maxDBConns, batchSize)

	// Create a channel for tasks
	taskChan := make(chan DumpTask, maxWorkers)

	// Create a wait group to wait for all workers to finish
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker(db, taskChan, &wg)
	}

  // Prepare the query based on whether a specific datadump_id was provided
	var rows *sql.Rows
  if *datadumpID > 0 {
		fmt.Printf("Processing specific datadump ID: %d\n", *datadumpID)
		rows, err = db.Query("SELECT id, tracker_module, tracker_slug, target_file, table_limit, table_sort, table_cond FROM trak_data_dumper_data WHERE id = ?", *datadumpID)
	} else {
		fmt.Println("Processing all datadump records...")
		rows, err = db.Query("SELECT id, tracker_module, tracker_slug, target_file, table_limit, table_sort, table_cond FROM trak_data_dumper_data")
	}

	if err != nil {
		log.Fatal("Error querying trak_data_dumper_data:", err)
	}
	defer rows.Close()

	// Enqueue tasks
  for rows.Next() {
		var task DumpTask
		err := rows.Scan(&task.DatadumpID, &task.TrackerModule, &task.TrackerSlug, &task.TargetFile, &task.TableLimit, &task.TableSort, &task.TableCond)
		if err != nil {
			log.Printf("Error scanning row: %v\n", err)
			continue
		}
		taskChan <- task
	}

	// Close the task channel
	close(taskChan)

	// Wait for all workers to finish
	wg.Wait()

	fmt.Println("Data dumping process completed.")

	elapsedTime := time.Since(startTime)
	fmt.Printf("Total execution time: %s\n", elapsedTime)
}

// Update the worker function to fetch TableLimit and TableSort
func worker(db *sql.DB, taskChan <-chan DumpTask, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range taskChan {
		if verbose {
			fmt.Printf("Processing datadump ID: %d\n", task.DatadumpID)
		}

		// Fetch TableLimit and TableSort
		err := db.QueryRow("SELECT table_limit, table_sort FROM trak_data_dumper_data WHERE id = ?", task.DatadumpID).Scan(&task.TableLimit, &task.TableSort)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error fetching table_limit and table_sort for datadump ID %d: %v\n", task.DatadumpID, err)
			continue
		}

		err = processTask(db, task)
		if err != nil {
			log.Printf("Error processing task for datadump ID %d: %v\n", task.DatadumpID, err)
		}
	}
}

func processTask(db *sql.DB, task DumpTask) error {
	// Search tracker table
	if verbose {
		fmt.Printf("Searching tracker table for module '%s' and slug '%s'...\n", task.TrackerModule, task.TrackerSlug)
	}
	var trackerId int
	var datatable, excelfields, listfields sql.NullString
	err := db.QueryRow("SELECT id, datatable, excelfields, listfields FROM tracker WHERE module = ? AND slug = ?", task.TrackerModule, task.TrackerSlug).Scan(&trackerId, &datatable, &excelfields, &listfields)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("error querying tracker table: %v", err)
	}

	var tableName string
	if err == sql.ErrNoRows || !datatable.Valid || datatable.String == "" {
		tableName = "trak_" + task.TrackerSlug + "_data"
		if verbose {
			fmt.Printf("No matching entry found in tracker table or datatable is NULL/empty. Using table name: %s\n", tableName)
		}
	} else {
		tableName = datatable.String
		if verbose {
			fmt.Printf("Matching entry found in tracker table. Using table name: %s\n", tableName)
		}
	}

	// Check if the table exists
	if !tableExists(db, tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Get the columns to be dumped
	var columnsToExport []string
	var columnSource string
	if excelfields.Valid && excelfields.String != "" {
		columnsToExport = strings.Split(excelfields.String, ",")
		columnSource = "excelfields"
	} else if listfields.Valid && listfields.String != "" {
		columnsToExport = strings.Split(listfields.String, ",")
		columnSource = "listfields"
	} else {
		return fmt.Errorf("no columns specified for export in table '%s'", tableName)
	}

	if verbose {
		fmt.Printf("Column source: %s\n", columnSource)
		fmt.Printf("Columns to export: %v\n", columnsToExport)
	}

	if len(columnsToExport) == 0 {
		return fmt.Errorf("no columns specified for export in table '%s'", tableName)
	}

	// Validate columns against tracker_field table and get field types
	validColumns, fieldTypes, fieldLabels, err := validateColumnsAndGetTypes(db, trackerId, columnsToExport)
	if err != nil {
		return fmt.Errorf("error validating columns for table '%s': %v", tableName, err)
	}

	if len(validColumns) == 0 {
		return fmt.Errorf("no valid columns found for export in table '%s'", tableName)
	}

	if verbose {
		fmt.Printf("Valid columns after validation: %v\n", validColumns)
	}

	// Dump data to Excel
  fmt.Printf("Dumping data from table '%s' to Excel...\n", tableName)
  if err := dumpToExcel(db, tableName, trackerId, validColumns, fieldTypes, fieldLabels, task.TrackerModule, task.TrackerSlug, task.TargetFile, task.TableLimit, task.TableSort, task.TableCond); err != nil {
		return fmt.Errorf("error dumping data to Excel for table '%s': %v", tableName, err)
	}

	fmt.Printf("Successfully dumped data from table '%s' to Excel.\n", tableName)
	return nil
}

func tableExists(db *sql.DB, tableName string) bool {
	query := `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_NAME = ?
	`
	var count int
	err := db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		log.Printf("Error checking if table exists: %v\n", err)
		return false
	}
	return count > 0
}

func validateColumnsAndGetTypes(db *sql.DB, trackerId int, columns []string) ([]string, map[string]string, map[string]string, error) {
	var validColumns []string
	fieldTypes := make(map[string]string)
	fieldLabels := make(map[string]string)
	for _, col := range columns {
		col = strings.TrimSpace(col)
		var count int
		var fieldType string
		var fieldLabel string
		err := db.QueryRow("SELECT COUNT(*), field_type, label FROM tracker_field WHERE tracker_id = ? AND name = ? GROUP BY field_type, label", trackerId, col).Scan(&count, &fieldType, &fieldLabel)
		if err != nil && err != sql.ErrNoRows {
			return nil, nil, nil, fmt.Errorf("error validating column %s: %v", col, err)
		}
		if count > 0 {
			validColumns = append(validColumns, col)
			fieldTypes[col] = fieldType
			fieldLabels[col] = fieldLabel
		}
	}
	return validColumns, fieldTypes, fieldLabels, nil
}

func getExcelColumnName(index int) string {
	var columnName string
	for index > 0 {
		index--
		columnName = string('A'+index%26) + columnName
		index = index / 26
	}
	return columnName
}

func processUserBranchField(value interface{}, stmt *sql.Stmt) interface{} {
	if value == nil {
		return ""
	}

	var id interface{}
	switch v := value.(type) {
	case int64:
		id = v
	case []uint8:
		id = string(v)
	case string:
		id = v
	default:
		return fmt.Sprintf("%v", value)
	}

	if id == "" {
		return ""
	}

	var name sql.NullString
	err := stmt.QueryRow(id).Scan(&name)
	if err == nil {
		if name.Valid {
			return name.String
		}
		return "" // Return empty string for NULL values
	} else if err != sql.ErrNoRows {
		fmt.Printf("Error looking up name for ID %v: %v\n", id, err)
	}

	return fmt.Sprintf("%v", id)
}

func formatDate(value interface{}) interface{} {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case time.Time:
		return v.Format("02/01/2006") // Format as dd/mm/yyyy
	case string:
		if t, err := time.Parse("2006-01-02T15:04:05Z", v); err == nil {
			return t.Format("02/01/2006")
		}
		return v // Return original value if parsing fails
	default:
		return fmt.Sprintf("%v", value)
	}
}

func formatInteger(value interface{}) interface{} {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return v
	case float64:
		return int(math.Round(v))
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int(math.Round(f))
		}
		return v // Return original value if parsing fails
	default:
		return fmt.Sprintf("%v", value)
	}
}

func dumpToExcel(db *sql.DB, tableName string, trackerId int, columns []string, fieldTypes map[string]string, fieldLabels map[string]string, module, slug, targetFile string, tableLimit sql.NullInt64, tableSort sql.NullString, tableCond sql.NullString) error {

	// Create a new Excel file
	f := excelize.NewFile()
	defer func() {
    if err := f.Close(); err != nil {
      log.Printf("Error closing Excel file: %v", err)
    }
  }()

	// Create a new sheet
	sheetName := "Data"
	_, err := f.NewSheet(sheetName)
	if err != nil {
		return fmt.Errorf("error creating new sheet: %v", err)
	}

	// Remove the default sheet
	defaultSheet := f.GetSheetName(0)
	if defaultSheet != sheetName {
		if err := f.DeleteSheet(defaultSheet); err != nil {
			return fmt.Errorf("error deleting default sheet: %v", err)
		}
	}

  // Set the active sheet
  index, err := f.GetSheetIndex(sheetName)
  if err != nil {
		return fmt.Errorf("error getting sheet index: %v", err)
	}
	f.SetActiveSheet(index)

	// Create a streaming writer
	streamWriter, err := f.NewStreamWriter(sheetName)
	if err != nil {
		return fmt.Errorf("error creating stream writer: %v", err)
	}

	// Write column headers using the streaming writer
	fmt.Println("Processing header rows...")
	headerRow := make([]interface{}, len(columns))
	for i, col := range columns {
		headerRow[i] = fieldLabels[col]
	}
	if err := streamWriter.SetRow("A1", headerRow); err != nil {
		return fmt.Errorf("error writing header row: %v", err)
	}
	fmt.Println("Header row processed successfully.")

	// Prepare statements for User and Branch lookups
	userStmt, err := db.Prepare("SELECT emp_name FROM IAP_User WHERE id = ?")
	if err != nil {
		return fmt.Errorf("error preparing user statement: %v", err)
	}
	defer userStmt.Close()

	branchStmt, err := db.Prepare("SELECT Branch_Name FROM Branch_Listing WHERE id = ?")
	if err != nil {
		return fmt.Errorf("error preparing branch statement: %v", err)
	}
	defer branchStmt.Close()

  // Prepare the query with OFFSET, FETCH, and optional ORDER BY and LIMIT
	baseQuery := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), tableName)

  // Add WHERE clause if tableCond is provided
	if tableCond.Valid && tableCond.String != "" {
		baseQuery += " WHERE " + tableCond.String
	}

  // Add ORDER BY clause if tableSort is provided
	if tableSort.Valid && tableSort.String != "" {
		baseQuery += " ORDER BY " + tableSort.String
	} else {
		baseQuery += " ORDER BY (SELECT NULL)"
	}

	// Add OFFSET and FETCH clauses
	baseQuery += " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"

	offset := 0
	rowIndex := 2
	totalRowsProcessed := 0
  var effectiveLimit int64

	// Determine if we should apply a limit
	if tableLimit.Valid && tableLimit.Int64 > 0 {
		effectiveLimit = tableLimit.Int64
	} else {
		effectiveLimit = -1 // No limit
	}

  for {
		// Determine the number of rows to fetch in this batch
    rowsToFetch := batchSize
		if effectiveLimit > 0 {
			remainingRows := effectiveLimit - int64(totalRowsProcessed)
			if remainingRows <= 0 {
				break // We've reached the table limit
			}
			if remainingRows < int64(batchSize) {
				rowsToFetch = int(remainingRows)
			}
		}

		// Query for a batch of rows
		rows, err := db.Query(baseQuery, offset, rowsToFetch)
		if err != nil {
			return fmt.Errorf("error querying table %s: %v", tableName, err)
		}

		rowsProcessed := 0

		for rows.Next() {
			// Create a slice of interface{} to hold the row data
			rowData := make([]interface{}, len(columns))
			rowPointers := make([]interface{}, len(columns))
			for i := range rowData {
				rowPointers[i] = &rowData[i]
			}

			// Scan the row into the slice
			if err := rows.Scan(rowPointers...); err != nil {
				rows.Close()
				return fmt.Errorf("error scanning row: %v", err)
			}

			// Process and write the row data to Excel
			cellValues := make([]interface{}, len(columns))
			for i, col := range columns {
				value := rowData[i]

        switch fieldTypes[col] {
				case "User":
					cellValues[i] = processUserBranchField(value, userStmt)
				case "Branch":
					cellValues[i] = processUserBranchField(value, branchStmt)
				case "Date":
					cellValues[i] = formatDate(value)
				case "Integer":
					cellValues[i] = formatInteger(value)
				default:
					cellValues[i] = value
				}

			}

			cell, _ := excelize.CoordinatesToCellName(1, rowIndex)
			if err := streamWriter.SetRow(cell, cellValues); err != nil {
				rows.Close()
				return fmt.Errorf("error writing row to Excel: %v", err)
			}

      rowIndex++
			rowsProcessed++
			totalRowsProcessed++

			// Check if we've reached the table limit
      if effectiveLimit > 0 && int64(totalRowsProcessed) >= effectiveLimit {
				rows.Close()
				break
			}
		}
		rows.Close()

		if rowsProcessed < rowsToFetch {
			break // We've processed all rows
		}

		offset += rowsProcessed

		// Flush the stream writer periodically to save memory
		/* if offset%100000 == 0 {
			if err := streamWriter.Flush(); err != nil {
				return fmt.Errorf("error flushing stream writer: %v", err)
			}
		} */
	}

	// Final flush of the stream writer
	if err := streamWriter.Flush(); err != nil {
		return fmt.Errorf("error flushing stream writer: %v", err)
	}

	// Save the Excel file with the new naming convention
  filename := targetFile
	if filename == "" {
		// Fallback to the old naming convention if target_file is empty
		filename = fmt.Sprintf("%s_%s.xlsx", module, slug)
	}

	if err := f.SaveAs(filename); err != nil {
		return fmt.Errorf("error saving Excel file: %v", err)
	}

	fmt.Printf("Data from table '%s' has been successfully exported to '%s'\n", tableName, filename)
	return nil

}

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/xuri/excelize/v2"
)

var verbose bool

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

	// Connect to the database
	fmt.Printf("Connecting to database %s on server %s...\n", *database, *server)
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Error connecting to the database:", err)
	}
	defer db.Close()
	fmt.Println("Connected successfully.")

	// Prepare the query based on whether a specific datadump_id was provided
	var rows *sql.Rows
	if *datadumpID > 0 {
		fmt.Printf("Processing specific datadump ID: %d\n", *datadumpID)
		rows, err = db.Query("SELECT id, tracker_module, tracker_slug FROM trak_data_dumper_data WHERE id = ?", *datadumpID)
	} else {
		fmt.Println("Processing all datadump records...")
		rows, err = db.Query("SELECT id, tracker_module, tracker_slug FROM trak_data_dumper_data")
	}

	if err != nil {
		log.Fatal("Error querying trak_data_dumper_data:", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		if verbose {
			fmt.Printf("Processing row %d from trak_data_dumper_data...\n", rowCount)
		}

		var datadumpID int
		var trackerModule, trackerSlug string
		err := rows.Scan(&datadumpID, &trackerModule, &trackerSlug)
		if err != nil {
			log.Printf("Error scanning row %d: %v\n", rowCount, err)
			continue
		}

		// Search tracker table
		if verbose {
			fmt.Printf("Searching tracker table for module '%s' and slug '%s'...\n", trackerModule, trackerSlug)
		}
		var trackerId int
		var datatable, excelfields, listfields sql.NullString
		err = db.QueryRow("SELECT id, datatable, excelfields, listfields FROM tracker WHERE module = ? AND slug = ?", trackerModule, trackerSlug).Scan(&trackerId, &datatable, &excelfields, &listfields)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error querying tracker table for row %d: %v\n", rowCount, err)
			continue
		}

		var tableName string
		if err == sql.ErrNoRows || !datatable.Valid || datatable.String == "" {
			tableName = "trak_" + trackerSlug + "_data"
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
			log.Printf("Table '%s' does not exist. Skipping...\n", tableName)
			continue
		}

		// Get the columns to be dumped
		var columnsToExport []string
		var columnSource string
		if excelfields.Valid && excelfields.String != "" {
			columnsToExport = strings.Split(excelfields.String, ",")
			columnSource = "excelfields"
			fmt.Printf("Columns determined by excelfields: %s\n", excelfields.String)
		} else if listfields.Valid && listfields.String != "" {
			columnsToExport = strings.Split(listfields.String, ",")
			columnSource = "listfields"
			fmt.Printf("Columns determined by listfields: %s\n", listfields.String)
		} else {
			fmt.Printf("No columns specified for export in table '%s'. Skipping...\n", tableName)
			continue
		}

		fmt.Printf("Column source: %s\n", columnSource)
		fmt.Printf("Columns to export: %v\n", columnsToExport)

		if len(columnsToExport) == 0 {
			log.Printf("No columns specified for export in table '%s'. Skipping...\n", tableName)
			continue
		}

		// Validate columns against tracker_field table and get field types
		validColumns, fieldTypes, err := validateColumnsAndGetTypes(db, trackerId, columnsToExport)
		if err != nil {
			log.Printf("Error validating columns for table '%s': %v\n", tableName, err)
			continue
		}

		if len(validColumns) == 0 {
			log.Printf("No valid columns found for export in table '%s'. Skipping...\n", tableName)
			continue
		}

		fmt.Printf("Valid columns after validation: %v\n", validColumns)

		// Dump data to Excel
		fmt.Printf("Dumping data from table '%s' to Excel...\n", tableName)
		if err := dumpToExcel(db, tableName, trackerId, validColumns, fieldTypes, trackerModule, trackerSlug); err != nil {
			log.Printf("Error dumping data to Excel for table '%s': %v\n", tableName, err)
		} else {
			fmt.Printf("Successfully dumped data from table '%s' to Excel.\n", tableName)
		}
	}

	fmt.Println("Data dumping process completed.")

	elapsedTime := time.Since(startTime)
	fmt.Printf("Total execution time: %s\n", elapsedTime)
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

func validateColumnsAndGetTypes(db *sql.DB, trackerId int, columns []string) ([]string, map[string]string, error) {
	var validColumns []string
	fieldTypes := make(map[string]string)
	for _, col := range columns {
		col = strings.TrimSpace(col)
		var count int
		var fieldType string
		err := db.QueryRow("SELECT COUNT(*), field_type FROM tracker_field WHERE tracker_id = ? AND name = ? GROUP BY field_type", trackerId, col).Scan(&count, &fieldType)
		if err != nil && err != sql.ErrNoRows {
			return nil, nil, fmt.Errorf("error validating column %s: %v", col, err)
		}
		if count > 0 {
			validColumns = append(validColumns, col)
			fieldTypes[col] = fieldType
		}
	}
	return validColumns, fieldTypes, nil
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

func dumpToExcel(db *sql.DB, tableName string, trackerId int, columns []string, fieldTypes map[string]string, module, slug string) error {
	// Prepare the query
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), tableName)
	if verbose {
		fmt.Printf("Executing query: %s\n", query)
	}
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying table %s: %v", tableName, err)
	}
	defer rows.Close()

  fmt.Printf("Columns to dump: %v\n", columns)

	// Get column labels from tracker_field table
	columnLabels := make(map[string]string)
	for _, col := range columns {
		var label string
		err := db.QueryRow("SELECT label FROM tracker_field WHERE tracker_id = ? AND name = ?", trackerId, col).Scan(&label)
		if err == nil {
			columnLabels[col] = label
		} else if err != sql.ErrNoRows {
			fmt.Printf("Error querying tracker_field for column %s: %v\n", col, err)
		}
	}

	// Create a new Excel file
	f := excelize.NewFile()
	defer f.Close()

	// Set column headers
	for i, col := range columns {
    cell := fmt.Sprintf("%s1", getExcelColumnName(i+1))
		label, exists := columnLabels[col]
		if exists {
			f.SetCellValue("Sheet1", cell, label)
		} else {
			f.SetCellValue("Sheet1", cell, col)
		}
    fmt.Printf("Column header %s\n", cell)
	}

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

	// Write data to Excel
	rowIndex := 2
	for rows.Next() {
		// Create a slice of interface{} to hold the row data
		rowData := make([]interface{}, len(columns))
		rowPointers := make([]interface{}, len(columns))
		for i := range rowData {
			rowPointers[i] = &rowData[i]
		}

		// Scan the row into the slice
		if err := rows.Scan(rowPointers...); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}

		// Process and write the row data to Excel
		for i, col := range columns {
      cell := fmt.Sprintf("%s%d", getExcelColumnName(i+1), rowIndex)
			value := rowData[i]

			// Handle User and Branch field types
			switch fieldTypes[col] {
			case "User":
				if id, ok := value.(int64); ok {
					var empName string
					err := userStmt.QueryRow(id).Scan(&empName)
					if err == nil {
						value = empName
					} else if err != sql.ErrNoRows {
						fmt.Printf("Error looking up user name for ID %d: %v\n", id, err)
					}
				}
			case "Branch":
				if id, ok := value.(int64); ok {
					var branchName string
					err := branchStmt.QueryRow(id).Scan(&branchName)
					if err == nil {
						value = branchName
					} else if err != sql.ErrNoRows {
						fmt.Printf("Error looking up branch name for ID %d: %v\n", id, err)
					}
				}
			}

			f.SetCellValue("Sheet1", cell, value)
		}
		rowIndex++
	}

	// Save the Excel file with the new naming convention
	filename := fmt.Sprintf("%s_%s.xlsx", module, slug)
	if err := f.SaveAs(filename); err != nil {
		return fmt.Errorf("error saving Excel file: %v", err)
	}

	fmt.Printf("Data from table '%s' has been successfully exported to '%s'\n", tableName, filename)
	return nil
}

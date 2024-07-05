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

func main() {
	// Define command-line flags
	server := flag.String("server", "", "Database server address")
	port := flag.Int("port", 1433, "Database server port")
	user := flag.String("user", "", "Database username")
	password := flag.String("password", "", "Database password")
	database := flag.String("database", "", "Database name")

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

	// Query trak_data_dumper_data table with updated column names
	fmt.Println("Querying trak_data_dumper_data table...")
	rows, err := db.Query("SELECT tracker_module, tracker_slug FROM trak_data_dumper_data")
	if err != nil {
		log.Fatal("Error querying trak_data_dumper_data:", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		fmt.Printf("Processing row %d from trak_data_dumper_data...\n", rowCount)

		var trackerModule, trackerSlug string
		err := rows.Scan(&trackerModule, &trackerSlug)
		if err != nil {
			log.Printf("Error scanning row %d: %v\n", rowCount, err)
			continue
		}

		// Search tracker table
		fmt.Printf("Searching tracker table for module '%s' and slug '%s'...\n", trackerModule, trackerSlug)
		var datatable string
		err = db.QueryRow("SELECT datatable FROM tracker WHERE module = ? AND slug = ?", trackerModule, trackerSlug).Scan(&datatable)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Error querying tracker table for row %d: %v\n", rowCount, err)
			continue
		}

		var tableName string
		if err == sql.ErrNoRows || datatable == "" {
			tableName = "trak_" + trackerSlug + "_data"
			fmt.Printf("No matching entry found in tracker table. Using table name: %s\n", tableName)
		} else {
			tableName = datatable
			fmt.Printf("Matching entry found in tracker table. Using table name: %s\n", tableName)
		}

		// Dump data to Excel
		fmt.Printf("Dumping data from table '%s' to Excel...\n", tableName)
		if err := dumpToExcel(db, tableName); err != nil {
			log.Printf("Error dumping data to Excel for table '%s': %v\n", tableName, err)
		} else {
			fmt.Printf("Successfully dumped data from table '%s' to Excel.\n", tableName)
		}
	}

	fmt.Println("Data dumping process completed.")
}

func dumpToExcel(db *sql.DB, tableName string) error {
	// Query all data from the table
	fmt.Printf("Querying all data from table '%s'...\n", tableName)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("error querying table %s: %v", tableName, err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error getting column names: %v", err)
	}
	fmt.Printf("Retrieved %d columns from table '%s'.\n", len(columns), tableName)

	// Create a new Excel file
	fmt.Println("Creating new Excel file...")
	f := excelize.NewFile()
	defer f.Close()

	// Set column headers
	fmt.Println("Setting column headers in Excel...")
	for i, col := range columns {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue("Sheet1", cell, col)
	}

	// Write data to Excel
	fmt.Println("Writing data to Excel...")
	rowIndex := 2
	rowCount := 0
	startTime := time.Now()
	for rows.Next() {
		rowCount++
		if rowCount%1000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
		}

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

		// Write the row data to Excel
		for i, val := range rowData {
			cell := fmt.Sprintf("%c%d", 'A'+i, rowIndex)
			f.SetCellValue("Sheet1", cell, val)
		}
		rowIndex++
	}
	duration := time.Since(startTime)
	fmt.Printf("Finished writing %d rows to Excel. Time taken: %v\n", rowCount, duration)

	// Save the Excel file
	filename := strings.ReplaceAll(tableName, " ", "_") + ".xlsx"
	fmt.Printf("Saving Excel file as '%s'...\n", filename)
	if err := f.SaveAs(filename); err != nil {
		return fmt.Errorf("error saving Excel file: %v", err)
	}

	fmt.Printf("Data from table '%s' has been successfully exported to '%s'\n", tableName, filename)
	return nil
}

package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type JSONItem struct {
	IPAddress string `json:"ip_address"`
	Port      string `json:"port"`
	Edge      string `json:"edge"`
}

func filterItemsByEdgeSQL(items []JSONItem, edge string) string {
	var conditions []string
	for _, item := range items {
		if item.Edge == edge {
			conditions = append(conditions, fmt.Sprintf("(pa.address = '%s' AND n.source_port = '%s')", item.IPAddress, item.Port))
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(conditions, " OR "))
}

func filterItemsByEdgeTesp01SQL(items []JSONItem, edge string) string {
	fmt.Println("Filtrando itens por edge...")
	var conditions []string
	for _, item := range items {
		if item.Edge == edge {
			conditions = append(conditions, fmt.Sprintf("(lb.public_ip = '%s' AND lbr.vip_port = '%s')", item.IPAddress, item.Port))
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(conditions, " OR "))
}

func main() {
	dbUser := "root"
	dbPassword := "root"
	dbName := "root"
	dbHost := "localhost"
	dbPort := "3306"

	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		fmt.Println("Erro ao abrir conexão com o banco de dados")
		log.Fatal(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Erro ao fechar conexão com o banco de dados")
			log.Fatal(err)
		}
	}(db)

	err = db.Ping()
	if err != nil {
		fmt.Println("Erro ao fazer ping com o banco de dados")
		log.Fatal(err)
	}

	// Lendo e filtrando itens do JSON
	fileContent, err := ioutil.ReadFile("ips.json")
	if err != nil {
		log.Fatal("Erro ao ler o arquivo JSON:", err)
	}

	var jsonItems []JSONItem
	err = json.Unmarshal(fileContent, &jsonItems)
	if err != nil {
		log.Fatal("Erro ao decodificar o JSON:", err)
	}

	var server = "TESP1"

	rows := standardServer(jsonItems, server, err, db)

	if server == "TESP1" {
		fmt.Println("Servidor TESP1")
		rows = tesp01dServer(jsonItems, server, err, db)
	}

	if err != nil {
		log.Fatal(err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(rows)

	csvFileName := "resultados.csv"
	var csvFile *os.File

	// Open the CSV file in append mode or create it if it doesn't exist
	csvFile, err = os.OpenFile(csvFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func(csvFile *os.File) {
		err := csvFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(csvFile)

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	info, err := csvFile.Stat()
	if err != nil {
		log.Fatal(err)
	}
	if info.Size() == 0 {
		headers := []string{"Public_Address", "Source_Port", "VM_Name", "Floating_IP"}
		if err := writer.Write(headers); err != nil {
			log.Fatal(err)
		}
	}

	for rows.Next() {
		fmt.Println("Lendo linha...")
		fmt.Println(rows)
		var coluna1, coluna2, coluna3, coluna4 string
		if err := rows.Scan(&coluna1, &coluna2, &coluna3, &coluna4); err != nil {
			log.Fatal(err)
		}

		row := []string{coluna1, coluna2, coluna3, coluna4}
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}

	writer.Flush()

	fmt.Printf("Resultados exportados para %s\n", csvFileName)
}

func standardServer(jsonItems []JSONItem, server string, err error, db *sql.DB) *sql.Rows {
	whereConditions := filterItemsByEdgeSQL(jsonItems, server)

	query := fmt.Sprintf(`
    	SELECT 
        	pa.address AS public_address, n.source_port, vm.name AS host, vm.floating_ip 
    	FROM public_address AS pa
    		INNER JOIN nats AS n ON n.public_address = pa.id
    		INNER JOIN virtual_machines AS vm ON vm.id = n.virtual_machine 
    	WHERE %s
	`, whereConditions)

	rows, err := db.Query(query)
	return rows
}

func tesp01dServer(jsonItems []JSONItem, server string, err error, db *sql.DB) *sql.Rows {
	whereConditions := filterItemsByEdgeTesp01SQL(jsonItems, server)

	query := fmt.Sprintf(`
			SELECT
				lb.public_ip AS 'public_address',
				lbr.vip_port AS 'source_port',
				i.name, 
				i.public_ip AS 'Floating_IP'
			FROM
				load_balances AS lb
			LEFT JOIN load_balances_rules AS lbr
					ON
				lbr.load_balance = lb.id
			LEFT JOIN instances AS i
				ON i.project = lbr.project 
    	WHERE %s
	`, whereConditions)

	fmt.Println("Query montada: %s", query)

	rows, err := db.Query(query)
	return rows
}

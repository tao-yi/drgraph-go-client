package main

import (
	"encoding/json"
	"fmt"

	"github.com/tao-yi/dgraph-go-client/pkg/dgraph"
)

type GetTableResponse struct {
	Tables []Table `json:"tables"`
}

type Table struct {
	Columns []Column `json:"columns"`
}

type Column struct {
	UID        string `json:"uid"`
	ColumnType string `json:"columnType"`
}

func main() {
	host := "localhost:9080"
	dgraphClient := dgraph.NewClient(host)
	defer dgraphClient.Cancel()

	query := `
	query GetTable($tableID: string) {
		tables(func: uid($tableID)) {
			columns: has_column {
				uid
				columnType
			}
		}
	}
	`

	tableID := "0x4ceb2"
	res, err := dgraphClient.QueryWithVars(query, map[string]string{"$tableID": tableID})
	if err != nil {
		panic(err)
	}

	result := GetTableResponse{}
	if err = json.Unmarshal(res, &result); err != nil {
		panic(err)
	}

	fmt.Printf("%+v", result)
}

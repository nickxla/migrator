/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"database/sql"
	"log"
	"time"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

type Title struct {
	name string
	from time.Time
	to   time.Time
}

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		openDatabase()
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dumpCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dumpCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func openDatabase() {
	db, err := sql.Open("mysql", "guest:relational@tcp(relational.fit.cvut.cz:3306)/employee?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("connection established")
	var (
		empNo  int
		title  string
		from   time.Time
		to     time.Time
		titles []Title
	)
	rows, err := db.Query("select * from titles where emp_no = ?", 110344)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&empNo, &title, &from, &to)
		if err != nil {
			log.Fatal(err)
		}
		titles = append(titles, Title{title, from, to})
	}
	for _, ttl := range titles {
		log.Printf("%s - %s to %s", ttl.name, ttl.from, ttl.to)
	}
	defer db.Close()
}

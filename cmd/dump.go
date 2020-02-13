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
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

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
		ids, err := InitIds()
		if err != nil {
			panic(err)
		}
		ids = ids[:50]
		log.Printf("Length of ids.txt : %d", len(ids))

		var (
			wg        sync.WaitGroup
			employees []Employee
		)
		ch := make(chan Employee)

		start := time.Now()
		for _, id := range ids {
			wg.Add(1)
			go DumpEmployee(id, &wg, ch)
		}

		go func() {
			for emp := range ch {
				employees = append(employees, emp)
			}
		}()

		wg.Wait()
		close(ch)
		InsertMany(employees)
		elapsed := time.Since(start)

		log.Printf("Dumped %d employees in %s", len(employees), elapsed)
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

func InitIds() ([]int, error) {
	file, err := os.Open("ids.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var ids []int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		i, err := strconv.Atoi(scanner.Text())
		if err != nil {
			panic(err)
		}
		ids = append(ids, i)
	}
	return ids, scanner.Err()
}

func GetManagerTitle(rows *sql.Rows, from time.Time, to time.Time) bool {
	var (
		deptNo   string
		empNo    int
		fromDate time.Time
		toDate   time.Time
	)
	for rows.Next() {
		err := rows.Scan(&deptNo, &empNo, &fromDate, &toDate)
		if err != nil {
			log.Fatal(err)
		}
		if fromDate == from && toDate == to {
			return true
		}
	}
	return false
}

func DumpTitles(id int) []Title {
	db, err := sql.Open("mysql", "guest:relational@tcp(relational.fit.cvut.cz:3306)/employee?parseTime=true")
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	var (
		empNo  int
		title  string
		from   time.Time
		to     time.Time
		titles []Title
	)
	rows, err := db.Query("select * from titles where emp_no = ?", id)
	if err != nil {
		log.Fatal(err)
	}
	deptManagers, err := db.Query("select * from dept_manager where emp_no = ?", id)
	for rows.Next() {
		err := rows.Scan(&empNo, &title, &from, &to)
		if err != nil {
			log.Fatal(err)
		}
		isManager := GetManagerTitle(deptManagers, from, to)
		titles = append(titles, Title{title, from, to, isManager})
	}
	return titles
}

func DumpSalaries(id int) []Salary {
	db, err := sql.Open("mysql", "guest:relational@tcp(relational.fit.cvut.cz:3306)/employee?parseTime=true")
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	var (
		amount   int
		from     time.Time
		to       time.Time
		salaries []Salary
	)
	rows, err := db.Query("select salary, from_date, to_date from salaries where emp_no = ?", id)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&amount, &from, &to)
		if err != nil {
			log.Fatal(err)
		}
		salaries = append(salaries, Salary{amount, from, to})
	}
	return salaries
}

func DumpEmployee(id int, wg *sync.WaitGroup, ch chan Employee) {
	defer wg.Done()
	db, err := sql.Open("mysql", "guest:relational@tcp(relational.fit.cvut.cz:3306)/employee?parseTime=true")
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	var (
		empNo     int
		birthDate time.Time
		firstName string
		lastName  string
		gender    string
		hireDate  time.Time
	)
	row := db.QueryRow("select * from employees where emp_no = ?", id)
	err = row.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
	if err != nil {
		log.Fatal(err)
	}
	titles := DumpTitles(id)
	salaries := DumpSalaries(id)
	current, history := DumpDepartments(id)
	ch <- Employee{empNo, birthDate, firstName, lastName, gender, hireDate, titles, salaries, current, history}
}

func DumpDepartments(id int) (Department, []Department) {
	db, err := sql.Open("mysql", "guest:relational@tcp(relational.fit.cvut.cz:3306)/employee?parseTime=true")
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	var (
		number  string
		from    time.Time
		to      time.Time
		name    string
		current Department
		history []Department
	)
	rows, err := db.Query("SELECT dpe.dept_no, from_date, to_date, dept_name FROM dept_emp dpe INNER JOIN departments d ON dpe.dept_no = d.dept_no WHERE emp_no = ? ORDER BY to_date", id)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		err := rows.Scan(&number, &from, &to, &name)
		if err != nil {
			log.Fatal(err)
		}
		if to.Year() == 9999 {
			current = Department{number, name, from, to}
		} else {
			history = append(history, Department{number, name, from, to})
		}
	}
	if current == (Department{}) {
		current = Department{"d000", "Retired", from, to}
	}
	return current, history
}

func InsertMany(employees []Employee) {
	// Declare host and port options to pass to the Connect() method
	mongoUri := fmt.Sprintf("mongodb+srv://%s:%s@cluster0-aub80.mongodb.net/test?retryWrites=true&w=majority", os.Getenv("MONGO_USER", os.Getenv("MONGO_PASSWORD")))
	clientOptions := options.Client().ApplyURI(mongoUri)

	// Connect to the MongoDB and return Client instance
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		fmt.Println("mongo.Connect() ERROR:", err)
		os.Exit(1)
	}

	// Access a MongoDB collection through a database
	col := client.Database("employee").Collection("employees")

	var emps []interface{}
	for _, emp := range employees {
		emps = append(emps, emp)
	}

	insertResult, err := col.InsertMany(context.TODO(), emps)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Inserted %d documents", len(insertResult.InsertedIDs))
}

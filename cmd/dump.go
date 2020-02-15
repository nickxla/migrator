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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	// mysql driver
	"github.com/cheggaaa/pb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

var db *sql.DB
var mgo *mongo.Client

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
		log.Printf("Length of ids.txt : %d", len(ids))

		var (
			wg        sync.WaitGroup
			employees []Employee
		)
		idsChunks := MakeChunksIds(ids)
		fmt.Printf("Length : %d", len(idsChunks))
		ch := make(chan []Employee)
		db, err = sql.Open("mysql", "root:nottheactualpassword@tcp(localhost:3306)/employee?parseTime=true")
		defer db.Close()
		db.SetMaxOpenConns(300)
		db.SetMaxIdleConns(runtime.NumCPU() * 8)

		start := time.Now()

		bar := pb.StartNew(len(idsChunks))
		for _, chunk := range idsChunks {
			wg.Add(1)
			go func(ids []int, ch chan []Employee) {
				defer wg.Done()
				DumpEmployees(ids, ch)
				bar.Increment()
			}(chunk, ch)
		}

		go func() {
			for empSlice := range ch {
				employees = append(employees, empSlice...)
			}
		}()

		wg.Wait()
		close(ch)
		bar.Finish()

		//chunks := MakeChunks(employees)

		/*
			// Declare host and port options to pass to the Connect() method
			mongoURI := fmt.Sprintf("mongodb://admin:admin@localhost:27017")
			clientOptions := options.Client().ApplyURI(mongoURI)

			// Connect to MongoDB
			mgo, err = mongo.Connect(context.TODO(), clientOptions)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Starting dumping into mongodb")

			ch2 := make(chan int)
			bar = pb.StartNew(len(chunks))
			for _, chunk := range chunks {
				wg.Add(1)
				go func(chunk []Employee, ch chan int) {
					defer wg.Done()
					InsertMany(chunk, ch2)
					bar.Increment()
				}(chunk, ch2)
			}

			var total int = 0
			go func() {
				for nbInsert := range ch2 {
					total += nbInsert
				}
			}()

			wg.Wait()
			close(ch2)
			bar.Finish()
		*/
		elapsed := time.Since(start)
		log.Printf("Dumped %d employees in %s" /* total,*/, len(employees), elapsed)
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

func ToJson(emps []Employee) {
	file, _ := json.MarshalIndent(emps, "", "")
	_ = ioutil.WriteFile("test.json", file, 0644)
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

func GetManagerTitle(deptManagers []Department, from time.Time, to time.Time) bool {
	for _, dept := range deptManagers {
		if dept.From == from && dept.To == to {
			return true
		}
	}
	return false
}

func DumpDeptManager(id int) []Department {
	var (
		number string
		empNo  int
		from   time.Time
		to     time.Time
		depts  []Department
	)
	rows, err := db.Query("select * from dept_manager where emp_no = ?", id)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&number, &empNo, &from, &to)
		if err != nil {
			log.Fatal(err)
		}
		depts = append(depts, Department{number, "", from, to})
	}
	rows.Close()
	return depts
}

func DumpTitles(id int, deptManagers []Department) []Title {
	var (
		empNo  int
		title  string
		from   time.Time
		to     time.Time
		titles []Title
	)
	rows, err := db.Query("select * from titles where emp_no = ?", id)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&empNo, &title, &from, &to)
		if err != nil {
			log.Fatal(err)
		}
		isManager := GetManagerTitle(deptManagers, from, to)
		titles = append(titles, Title{title, from, to, isManager})
	}
	rows.Close()
	return titles
}

func DumpSalaries(id int) []Salary {
	var (
		amount   int
		from     time.Time
		to       time.Time
		salaries []Salary
	)
	rows, err := db.Query("select salary, from_date, to_date from salaries where emp_no = ?", id)
	defer rows.Close()
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
	rows.Close()
	return salaries
}

func DumpEmployee(id int, titles []Title, salaries []Salary, current Department, history []Department) Employee {
	var (
		empNo     int
		birthDate time.Time
		firstName string
		lastName  string
		gender    string
		hireDate  time.Time
	)
	rows, err := db.Query("select * from employees where emp_no = ?", id)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		err := rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			log.Fatal(err)
		}
	}
	rows.Close()
	return Employee{empNo, birthDate, firstName, lastName, gender, hireDate, titles, salaries, current, history}
}

func DumpEmployees(ids []int, ch chan []Employee) {
	var employees []Employee
	for _, id := range ids {
		deptManagers := DumpDeptManager(id)
		titles := DumpTitles(id, deptManagers)
		salaries := DumpSalaries(id)
		current, history := DumpDepartments(id)
		employee := DumpEmployee(id, titles, salaries, current, history)
		employees = append(employees, employee)
	}
	ch <- employees
}

func DumpDepartments(id int) (Department, []Department) {
	var (
		number  string
		from    time.Time
		to      time.Time
		name    string
		current Department
		history []Department
	)
	rows, err := db.Query("SELECT dpe.dept_no, from_date, to_date, dept_name FROM dept_emp dpe INNER JOIN departments d ON dpe.dept_no = d.dept_no WHERE emp_no = ? ORDER BY to_date", id)
	defer rows.Close()
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
	rows.Close()
	return current, history
}

func MakeChunks(employees []Employee) [][]Employee {
	var divided [][]Employee
	numCPU := runtime.NumCPU()

	chunkSize := (len(employees) + numCPU - 1) / numCPU

	for i := 0; i < len(employees); i += chunkSize {
		end := i + chunkSize

		if end > len(employees) {
			end = len(employees)
		}

		divided = append(divided, employees[i:end])
	}

	var (
		results []int
		total   int = 0
	)
	for _, i := range divided {
		results = append(results, len(i))
	}
	for _, i := range results {
		total += i
	}

	avg := float64(total) / float64(len(results))
	log.Printf("Created %d chunks of average size : %f", len(divided), avg)
	return divided
}

func MakeChunksIds(ids []int) [][]int {
	var divided [][]int

	chunkSize := len(ids) / 95

	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize

		if end > len(ids) {
			end = len(ids)
		}

		divided = append(divided, ids[i:end])
	}

	var (
		results []int
		total   int = 0
	)
	for _, i := range divided {
		results = append(results, len(i))
	}
	for _, i := range results {
		total += i
	}

	avg := float64(total) / float64(len(results))
	log.Printf("Created %d chunks of average size : %f", len(divided), avg)
	return divided
}

func InsertMany(chunk []Employee, ch chan int) {
	// Access a MongoDB collection through a database
	col := mgo.Database("employee").Collection("employees")

	var emps []interface{}
	for _, emp := range chunk {
		emps = append(emps, emp)
	}
	insertResult, err := col.InsertMany(context.TODO(), emps)
	if err != nil {
		log.Fatal(err)
	}
	ch <- len(insertResult.InsertedIDs)
}

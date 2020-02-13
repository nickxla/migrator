package cmd

import "time"

type Employee struct {
	EmpNo       int          `json:"emp_no" bson:"emp_no"`
	BirthDate   time.Time    `json:"birth_date" bson:"birth_date"`
	FirstName   string       `json:"first_name" bson:"first_name"`
	LastName    string       `json:"last_name" bson:"last_name"`
	Gender      string       `json:"gender" bson:"gender"`
	HireDate    time.Time    `json:"hire_date" bson:"hire_date"`
	Titles      []Title      `json:"titles" bson:"titles"`
	Salaries    []Salary     `json:"salaries" bson:"salaries"`
	DeptCurrent Department   `json:"current_dept" bson:"current_dept"`
	DeptHistory []Department `json:"dept_history,omitempty" bson:"dept_history"`
}

type Title struct {
	Name      string    `json:"name" bson:"name"`
	From      time.Time `json:"from_date" bson:"from_date"`
	To        time.Time `json:"to_date" bson:"to_date"`
	IsManager bool      `json:"is_manager" bson:"is_manager"`
}

type Salary struct {
	Amount int       `json:"amount" bson:"amount"`
	From   time.Time `json:"from_date" bson:"from_date"`
	To     time.Time `json:"to_date" bson:"to_date"`
}

type Department struct {
	Number string    `json:"dept_no" bson:"dept_no"`
	Name   string    `json:"dept_name" bson:"dept_name"`
	From   time.Time `json:"from_date" bson:"from_date"`
	To     time.Time `json:"to_date,omitempty" bson:"to_date,omitempty"`
}

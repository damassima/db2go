package main

import (
	_ "bitbucket.org/phiggins/go-db2-cli"
	"database/sql"
	"flag"
	"fmt"
	"os"
)

var (
	connStr = flag.String("conn", "", "connection string to use")
	sqls    = flag.String("sql", "", "sql")
	repeat  = flag.Uint("repeat", 1, "number of times to repeat query")
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: %s [options]

%s connects to DB2 and executes a simple SQL statement a configurable
number of times.

Here is a sample connection string:

DATABASE=MYDBNAME; HOSTNAME=localhost; PORT=60000; PROTOCOL=TCPIP; UID=username; PWD=password;
`, os.Args[0], os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func execQuery(st *sql.Stmt) error {
	rows, err := st.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c interface{}
		err = rows.Scan(&c)
		if err != nil {
			return err
		}
		fmt.Println(c)
	}
	return rows.Err()
}

func dbOperations() error {
	db, err := sql.Open("db2-cli", *connStr)
	if err != nil {
		return err
	}
	defer db.Close()
	st, err := db.Prepare(*sqls)
	if err != nil {
		return err
	}
	defer st.Close()

	for i := 0; i < int(*repeat); i++ {
		err = execQuery(st)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *connStr == "" {
		fmt.Fprintln(os.Stderr, "-conn is required")
		flag.Usage()
	}
	if *sqls == "" {
		fmt.Fprintln(os.Stderr, "-sql is required")
		flag.Usage()
	}
	if err := dbOperations(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

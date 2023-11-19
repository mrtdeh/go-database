package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	client *DBClient
)

type DBClient struct {
	conn *sql.DB
	cnf  *Config
}

type ErrorFn func(err error)

var handleError ErrorFn
var lastErr string

func genHandleError(inFn ErrorFn) ErrorFn {
	return func(err error) {
		if inFn != nil {
			if err.Error() != lastErr {
				inFn(err)
				lastErr = err.Error()
			}
		}
	}
}

func newConnection(cnf *Config) error {
	// return back if none empty
	if client != nil {
		return nil
	}

	handleError = genHandleError(cnf.OnError)

	var err error
	ctxBG := context.Background()

	// config context timeout
	ctxConnTimeout, cancel := context.WithTimeout(ctxBG, 3*time.Second)
	defer cancel()
	// load TLS config
	tlsConfig := cnf.TLSConfig

	// config connection paramaters
	dbConfig := &mysql.Config{
		User:                 cnf.User,
		Passwd:               cnf.Pass,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", cnf.Host, cnf.Port),
		DBName:               "",
		Timeout:              3 * time.Second,
		AllowNativePasswords: true,
	}

	// add tls config if enabled
	if tlsConfig != nil {
		mysql.RegisterTLSConfig("siem", tlsConfig)
		dbConfig.TLSConfig = "siem"
	}

	// create a temp connection to test and create databse
	tmpConn, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return err
	}
	defer tmpConn.Close()

	// ping to mysql server with custom context
	err = tmpConn.PingContext(ctxConnTimeout)
	if err != nil {
		return err
	}
	// create database table if not exist
	_, err = tmpConn.Exec(fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;`, cnf.DBName))
	if err != nil {
		return errors.New("error creating database: " + err.Error())
	}
	// test to use database
	_, err = tmpConn.Exec(fmt.Sprintf(`USE %s;`, cnf.DBName))
	if err != nil {
		return errors.New("error selecting database: " + err.Error())
	}

	// set database name
	dbConfig.DBName = cnf.DBName
	// real connect to mysql
	conn, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return err
	}
	client = &DBClient{conn, cnf}

	go client.pingHandler(ctxConnTimeout, time.Second*5)

	log.Println("successfuly connect ot mariadb")
	return nil
}

func (c *DBClient) pingHandler(ctx context.Context, dur time.Duration) {
	for {
		err := c.conn.PingContext(ctx)
		if err != nil {
			handleError(err)
			log.Println("ping err : ", err.Error())
		}

		time.Sleep(dur)
	}
}

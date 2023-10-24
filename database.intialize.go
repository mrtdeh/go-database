package database

import (
	"crypto/tls"
	"database/sql"
	"errors"
)

type Config struct {
	Host      string
	Port      int
	User      string
	Pass      string
	DBName    string
	Migrator  func(conn *sql.DB) error
	TLSConfig *tls.Config
}

func Init(cnf *Config) error {
	if cnf == nil {
		cnf = &Config{
			Host: "localhost",
			Pass: "",
			Port: 3306,
			User: "root",
		}
	}

	err := newConnection(cnf)
	if err != nil {
		return errors.New("error connecting to database: " + err.Error())
	}

	if cnf.Migrator != nil {
		return cnf.Migrator(client.conn)
	}

	return nil
}

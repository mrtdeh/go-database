package database

import (
	"crypto/tls"
	"errors"
)

type Config struct {
	Host      string
	Port      int
	User      string
	Pass      string
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

	_, err = Mysql.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INT NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			type VARCHAR(32),
			status VARCHAR(32) NOT NULL,
			description TEXT,
			PRIMARY KEY (id)
		);
	`)
	if err != nil {
		return errors.New("error creating events table: " + err.Error())
	}

	_, err = Mysql.Exec(`
		CREATE TABLE IF NOT EXISTS threats (
			id INT NOT NULL AUTO_INCREMENT,
			event_id INT NOT NULL,
			value VARCHAR(255) NOT NULL,
			FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
			PRIMARY KEY (id)
		);
	`)
	if err != nil {
		return errors.New("error creating threats table: " + err.Error())
	}

	return nil
}

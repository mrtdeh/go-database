package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"log"
	"time"
)

type ErrorFn func(err error)

type Config struct {
	Host              string
	Port              int
	User              string
	Pass              string
	DBName            string
	Migrator          func(conn *sql.DB) error
	TLSConfig         *tls.Config
	IdentifierTag     string
	OnConnectingError ErrorFn
	OnConnected       func()
}

var handleConnect func()
var handleError ErrorFn
var lastErr error

func genHandleError(inFn ErrorFn) ErrorFn {
	return func(err error) {
		if inFn != nil {
			var e1, e2 string
			if err != nil {
				e1 = err.Error()
			}
			if lastErr != nil {
				e2 = lastErr.Error()
			}
			if e1 == context.DeadlineExceeded.Error() {
				return
			}
			if e1 != e2 {
				if err != nil {
					inFn(err)
				}
			}
			lastErr = err
		}
	}
}

func (c *DBClient) pingHandler(ctx context.Context, dur time.Duration) {
	var connected bool
	for {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := c.conn.PingContext(ctx)
			if err != nil {
				connected = false
				handleError(err)
				log.Println("ping err : ", err)
				return
			}

			if handleConnect != nil && !connected {
				handleConnect()
				connected = true
			}

			handleError(nil)
		}()

		time.Sleep(dur)
	}
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

	handleError = genHandleError(cnf.OnConnectingError)
	handleConnect = cnf.OnConnected

	err := newConnection(cnf)
	if err != nil {
		return errors.New("error connecting to database: " + err.Error())
	}

	if cnf.Migrator != nil {
		return cnf.Migrator(client.conn)
	}

	return nil
}

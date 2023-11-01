package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/blockloop/scan/v2"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

const (
	InserActionNone    = 0
	InserActionIgnore  = 1
	InserActionReplace = 2
)

var (
	Build b
)

type (
	b            struct{}
	InsertAction int

	QueryOption struct {
		TableName    string
		Record       interface{}
		Id           interface{}
		IdField      string
		InsertAction InsertAction
		// WithValues   bool
	}

	Rows struct {
		Keys []string
		Vals []any
	}

	RowsResult struct {
		Records *sql.Rows
		Error   error
	}

	Statement struct {
		table string
		conds []string
		err   error
	}

	Transaction struct {
		statements []string
	}
)

// ===============================================================================
func NewTransaction() *Transaction {
	return &Transaction{}
}

func (t *Transaction) Commit() {
	fmt.Println("commit : \n", strings.Join(t.statements, "\n"))
}

func (t *Transaction) SelectInto(column, from, where string) {
	if where != "" {
		where = "WHERE " + where
	}
	sql := "SELECT %s FROM %s %s FOR UPDATE;"
	t.statements = append(t.statements, fmt.Sprintf(sql, column, from, where))
}

func (t *Transaction) Update(column, from, where string) {
	if where != "" {
		where = "WHERE " + where
	}
	sql := "SELECT %s FROM %s %s FOR UPDATE;"
	t.statements = append(t.statements, fmt.Sprintf(sql, column, from, where))
}

func (t *Transaction) If(cond string) {
	t.statements = append(t.statements, fmt.Sprintf("IF (%s) THEN", cond))
}

func (t *Transaction) Else() {
	t.statements = append(t.statements, "ELSE")
}

func (t *Transaction) EndIf() {
	t.statements = append(t.statements, "END IF")
}

func (t *Transaction) Append(staements ...string) {
	for _, s := range staements {
		t.statements = append(t.statements, s)
	}
}

// =================================================================================

func GetOrAll(table string, id any, idField string) *RowsResult {
	var res *sql.Rows
	var err error

	if id != nil && !reflect.ValueOf(id).IsZero() {
		if strId, ok := id.(string); ok {
			id = "'" + strId + "'"
		}
		res, err = client.conn.Query(fmt.Sprintf("select * from %s where %s=%v;", table, idField, id))
		if err != nil {
			return &RowsResult{nil, err}
		}

	} else {

		res, err = client.conn.Query(fmt.Sprintf("select * from %s;", table))
		if err != nil {
			return &RowsResult{nil, err}
		}
	}

	return &RowsResult{res, nil}
}

func Query(query string, args ...any) (*sql.Rows, error) {
	return client.conn.Query(query, args...)
}

func QueryRow(query string, args ...any) *sql.Row {
	return client.conn.QueryRow(query, args...)
}

func Exec(query string, args ...any) (sql.Result, error) {
	return client.conn.Exec(query, args...)
}

//=========================== Search Statement =================================

func SearchIn(tbl string) *Statement {
	return &Statement{
		table: tbl,
	}
}

func (s *Statement) rollback() {
	cl := len(s.conds)
	if cl > 0 {
		st := strings.ToLower(s.conds[cl-1])
		if st == "and" || st == "or" {
			s.conds = s.conds[:cl-1]
		}
	}
}

func (s *Statement) like(field string, not bool, likes ...string) *Statement {
	likeOprator := "LIKE"
	if not {
		likeOprator = "NOT LIKE"
	}

	likes = cleanEmptyString(likes)

	if len(likes) == 0 {
		s.rollback()
		return s
	}

	for i := 0; i < len(likes); i++ {
		likes[i] = escape(likes[i])
	}

	orLikes := strings.Join(likes, "' OR ip LIKE  '")
	likeCond := fmt.Sprintf("(%s %s '%s')", field, likeOprator, orLikes)
	s.conds = append(s.conds, likeCond)
	return s
}

func (s *Statement) FieldLike(field string, likes ...string) *Statement {
	return s.like(field, false, likes...)
}

func (s *Statement) FieldNotLike(field string, notLikes ...string) *Statement {
	return s.like(field, true, notLikes...)
}

func (s *Statement) LikeIt(a interface{}) *Statement {

	c := reflect.TypeOf(a)
	sf := []string{}
	values := reflect.ValueOf(a)

	likeBuilder := func(key string, value any) (s string) {
		casei := "COLLATE utf8mb3_bin"
		if value != nil {
			f := reflect.ValueOf(value)
			t := f.Kind()

			if t == reflect.Pointer && f.IsNil() {
				return
			}

			switch t {
			case reflect.String:
				if value.(string) == "" {
					return
				}
				return fmt.Sprintf("%s %s LIKE '%%%s%%'", key, casei, escape(value.(string)))
			default:
				return fmt.Sprintf("%s=%v", key, value)
			}
		}
		return ""
	}

	if c.Kind() == reflect.Struct {

		for i := 0; i < c.NumField(); i++ {
			f := c.Field(i)
			dbTag := f.Tag.Get(client.cnf.IdentifierTag)
			if dbTag != "" {
				value := values.Field(i).Interface()
				sf = append(sf, likeBuilder(dbTag, value))
			}
		}
	} else if c.Kind() == reflect.Map {
		for key, value := range a.(map[string]interface{}) {
			sf = append(sf, likeBuilder(key, value))
		}
	}

	query := fmt.Sprintf("%s", strings.Join(sf, " AND "))

	if len(sf) == 0 {
		s.rollback()
		return s
	}
	s.conds = append(s.conds, query)
	return s
}

func (s *Statement) Where(conditions string, values ...string) *Statement {
	cc := strings.Count(conditions, "?")
	if cc != len(values) {
		s.rollback()
		s.err = fmt.Errorf("values count not equal to positions in where statement")
		return s
	}
	for i := 0; i < len(values); i++ {
		conditions = strings.Replace(conditions, "?", escape(values[i]), 1)
	}
	s.conds = append(s.conds, fmt.Sprintf("(%s)", conditions))
	return s
}

func (s *Statement) And() *Statement {
	s.conds = append(s.conds, "AND")
	return s
}

func (s *Statement) Or() *Statement {
	s.conds = append(s.conds, "OR")
	return s
}

func (s *Statement) Do() *RowsResult {
	var conds, where string
	if len(s.conds) > 0 {
		where = "WHERE"
		conds = strings.Join(s.conds, " ")
	}

	query := fmt.Sprintf("SELECT * FROM %s %s %s", s.table, where, conds)
	if s.err != nil {
		return &RowsResult{nil, s.err}
	}

	res, err := client.conn.Query(query)
	if err != nil {
		return &RowsResult{nil, err}
	}
	return &RowsResult{res, nil}
}

//=========================== Search Statement =================================

func (r *RowsResult) Scan(record interface{}) error {

	rows := r.Records
	recordValue := reflect.ValueOf(record)

	if recordValue.Kind() != reflect.Ptr || recordValue.IsNil() {
		return fmt.Errorf("record must be a valid pointer and not nil")
	}

	if rows == nil {
		return fmt.Errorf("rows is empty")
	}
	defer rows.Close()

	recordElem := recordValue.Elem()
	var err error
	switch recordElem.Kind() {
	case reflect.Struct:
		err = scan.Row(record, rows)
	case reflect.Array, reflect.Slice:
		err = scan.Rows(record, rows)
	}
	if err != nil {
		return fmt.Errorf("scan err : %s", err.Error())
	}

	return nil
}

// ================================================================================

func Exist(table string, id any, idField string) bool {
	statement := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", table, idField)

	// Execute the query
	var count int
	err := client.conn.QueryRow(statement, id).Scan(&count)
	if err != nil {
		panic(err)
	}

	// Check if the record exists or not
	if count > 0 {
		return true
	} else {
		return false
	}
}

func SafeUpsert(table string, obj interface{}, id any, idField string) (any, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Struct {
		log.Fatal("Object is not a struct")
		return nil, errors.New("Object is not a struct")
	}

	opt := QueryOption{
		TableName:    table,
		Record:       obj,
		Id:           id,
		IdField:      idField,
		InsertAction: InserActionNone,
		// WithValues:   false,
	}

	var query string
	var values []interface{}
	var existed bool

	existed = Exist(table, id, idField)

	if existed {
		query, values = updateStatement(opt)
	} else {
		query, values = insertStatement(opt)
	}

	res, err := client.conn.Exec(query, values...)
	if err != nil {
		// Rollback the transaction if an error occurs
		return nil, err
	}

	// Check if the INSERT operation was successful
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected > 0 && !existed {
		return id, nil
	}

	return nil, nil
}

func Upsert(table string, obj interface{}, id interface{}, idField string) error {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Struct {
		log.Fatal("Object is not a struct")
		return errors.New("Object is not a struct")
	}
	idref := reflect.ValueOf(id)

	if id != nil && !idref.IsZero() {
		err := Update(table, obj, id, idField)
		if err != nil {
			return err
		}
	} else {
		_, err := Create(table, obj, InserActionNone)
		if err != nil {
			return err
		}
	}

	return nil
}

func Create(table string, obj interface{}, action InsertAction) (sql.Result, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Struct {
		log.Fatal("Object is not a struct")
		return nil, errors.New("Object is not a struct")
	}

	stmt, values := insertStatement(QueryOption{
		TableName:    table,
		Record:       obj,
		InsertAction: action,
	})

	res, err := client.conn.Exec(stmt, values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func Update(table string, obj interface{}, id interface{}, idField string) error {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Struct {
		log.Fatal("Object is not a struct")
		return errors.New("Object is not a struct")
	}

	stmt, values := updateStatement(QueryOption{
		TableName: table,
		Record:    obj,
		Id:        id,
		IdField:   idField,
	})

	_, err := client.conn.Exec(stmt, values...)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func CreateMulti(table string, rows []interface{}, action InsertAction) error {
	var values []interface{}
	fetchKVs := prepareFetchKVsFunc([]string{"id"}, false)

	keys := fetchKVs(rows[0]).Keys
	keystr := strings.Join(keys, ",")
	sql := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES ", table, keystr)

	valCount := len(keys)
	newRows := int(len(rows))
	for i := 0; i < newRows; i++ {
		sql = sql + "( " + strings.Repeat("?,", valCount-1) + " ? ) ,"
		vals := fetchKVs(rows[i]).Vals
		values = append(values, vals...)
	}

	sql = sql[:len(sql)-1]

	if tx, err := client.conn.Begin(); err == nil {

		defer func() {
			if err := recover(); err != nil {
				fmt.Println("Failed to create multiple record:", err)
				tx.Rollback()
			}
		}()

		stmtIns, err := tx.Prepare(sql)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmtIns.Close()

		_, err = stmtIns.Exec(values...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("exec error : %s ", err.Error())
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("commit error : %s ", err.Error())
		}
		return nil
	} else {
		fmt.Println("Unable to open a connection to the database:", err.Error())
		return fmt.Errorf("Unable to open a connection to the database:%s", err.Error())
	}
}

func UpsertMulti(table string, rows []interface{}, updateKeys []string) error {
	if len(updateKeys) == 0 {
		return fmt.Errorf("update keys not specified")
	}

	fetchKVs := prepareFetchKVsFunc([]string{"id"}, true)

	if tx, err := client.conn.Begin(); err == nil {

		defer func() {
			if err := recover(); err != nil {
				fmt.Println("Failed to create multiple record:", err)
				tx.Rollback()
			}
		}()

		for _, r := range rows {

			f := fetchKVs(r)
			keys := f.Keys
			vals := f.Vals

			valstr := strings.Repeat("?,", len(vals))
			valstr = valstr[:len(valstr)-1]

			keystr := strings.Join(keys, ",")

			sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE ",
				table, keystr, valstr)
			for _, f := range updateKeys {

				for i, k := range keys {
					if k == f && (vals[i] != nil && vals[i] != "") {
						sql = sql + fmt.Sprintf(" %s = VALUES(%s),", k, k)
						break
					}
				}

			}
			c := sql[len(sql)-1:]
			if c == "," {
				sql = sql[:len(sql)-1]
			}

			// fmt.Println("sql : ", sql)

			_, err = tx.Exec(sql, vals...)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("exec error : %s ", err.Error())
			}
		}
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("commit error : %s ", err.Error())
		}
		return nil
	} else {
		fmt.Println("Unable to open a connection to the database:", err.Error())
		return fmt.Errorf("Unable to open a connection to the database:%s", err.Error())
	}
}

func Delete(table string, id any, idField string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s=?;", table, idField)
	_, err := client.conn.Exec(stmt, id)
	if err != nil {
		return err
	}
	return nil
}

func ParseErr(err error) string {

	var mysqlErr *mysql.MySQLError
	errors.As(err, &mysqlErr)

	switch mysqlErr.Number {
	case 1062:
		return "asset you want to add is duplicated, check if asset ip and mac are unique."
	}

	return err.Error()
}

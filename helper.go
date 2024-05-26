package database

import (
	"fmt"
	"reflect"
	"strings"
)

func prepareFetchKVsFunc(ignores []string, ignoreEmpties bool) func(any) Rows {
	return func(a any) (rows Rows) {
		c := reflect.TypeOf(a)
		values := reflect.ValueOf(a)

		for i := 0; i < c.NumField(); i++ {
			if !values.Field(i).CanInterface() {
				continue
			}
			value := values.Field(i).Interface()
			f := c.FieldByIndex([]int{i})

			if strContains(f.Name, ignores) {
				continue
			}

			if ignoreEmpties && isEmpty(value) {
				continue
			}

			j := f.Tag.Get(client.cnf.IdentifierTag)
			if j == "" {
				continue
			}
			rows.Keys = append(rows.Keys, j)

			if f.Type.Kind() == reflect.Bool {
				if value.(bool) {
					value = 1
				} else {
					value = 0
				}
			}
			rows.Vals = append(rows.Vals, value)

		}
		return rows
	}
}

func strContains(str string, arrStr []string) bool {
	for _, s := range arrStr {
		if strings.EqualFold(s, str) {
			return true
		}
	}
	return false
}

func isEmpty(myVar interface{}) bool {
	val := reflect.ValueOf(myVar)
	return val.Kind() == reflect.Ptr && val.IsNil() || val.Kind() == reflect.Chan && val.IsNil() || val.Kind() == reflect.Func && val.IsNil() ||
		(val.Kind() == reflect.Array || val.Kind() == reflect.Map || val.Kind() == reflect.Slice || val.Kind() == reflect.String) && val.Len() == 0 ||
		val.Kind() >= reflect.Int && val.Kind() <= reflect.Float64 && val.IsZero()
}

func insertStatement(qo QueryOption) (string, []interface{}) {

	tblname := qo.TableName
	data := qo.Record
	action := qo.InsertAction

	d := prepareFetchKVsFunc([]string{"id", "info_hash"}, true)(data)
	vals := strings.Repeat("?, ", len(d.Vals)-1) + "?"
	keys := strings.Join(d.Keys, ",")

	ignoreCmd := ""
	insertCmd := "INSERT"

	if action == InserActionIgnore {
		ignoreCmd = "IGNORE"
	} else if action == InserActionReplace {
		insertCmd = "REPLACE"
	}

	return fmt.Sprintf("%s %s INTO %s (%s) VALUES (%s)",
		insertCmd,
		ignoreCmd,
		tblname,
		keys,
		vals,
	), d.Vals
}

func updateStatement(qo QueryOption) (string, []interface{}) {

	tblname := qo.TableName
	data := qo.Record
	id := qo.Id
	idField := qo.IdField

	d := prepareFetchKVsFunc([]string{"id", "info_hash"}, true)(data)

	setClause := strings.Join(d.Keys, "=?,") + "=?"

	if strId, ok := id.(string); ok && strId != "" {
		id = "'" + strId + "'"
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s=%v", tblname, setClause, idField, id), d.Vals
}

func escape(source string) string {
	var j int = 0
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
		case '\n':
			flag = true
			escape = '\n'
		case '\\':
			flag = true
			escape = '\\'
		case '\'':
			flag = true
			escape = '\''
		case '"':
			flag = true
			escape = '"'
		case '\032':
			flag = true
			escape = 'Z'
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}

	escaped := string(desc[0:j])

	escaped = strings.ReplaceAll(escaped, "*", "%")
	escaped = strings.TrimSpace(escaped)

	return escaped
}

func cleanEmptyString(s []string) []string {
	var arr2 []string
	for _, str := range s {
		if str != "" {
			arr2 = append(arr2, str)
		}
	}
	return arr2
}

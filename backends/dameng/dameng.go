// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dameng

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	_ "gitee.com/chunanyong/dm"

	"yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/gotypes"
	"yunion.io/x/pkg/tristate"
	"yunion.io/x/pkg/util/regutils"

	"yunion.io/x/sqlchemy"
)

func init() {
	sqlchemy.RegisterBackend(&SDamengBackend{})
}

type SDamengBackend struct {
	sqlchemy.SBaseBackend
}

func (dameng *SDamengBackend) Name() sqlchemy.DBBackendName {
	return sqlchemy.DamengBackend
}

// CanUpdate returns wether the backend supports update
func (dameng *SDamengBackend) CanUpdate() bool {
	return true
}

// CanInsert returns wether the backend supports Insert
func (dameng *SDamengBackend) CanInsert() bool {
	return true
}

func (dameng *SDamengBackend) CanSupportRowAffected() bool {
	return true
}

// CanInsertOrUpdate returns weather the backend supports InsertOrUpdate
func (dameng *SDamengBackend) CanInsertOrUpdate() bool {
	return true
}

func (dameng *SDamengBackend) InsertOrUpdateSQLTemplate() string {
	// use PrepareInsertOrUpdateSQL instead
	return ""
}

func (dameng *SDamengBackend) DropIndexSQLTemplate() string {
	return `DROP INDEX "{{ .Index }}"" ON "{{ .Table }}"`
}

func (dameng *SDamengBackend) InsertSQLTemplate() string {
	return `INSERT INTO "{{ .Table }}" ({{ .Columns }}) VALUES ({{ .Values }})`
}

func (dameng *SDamengBackend) UpdateSQLTemplate() string {
	return `UPDATE "{{ .Table }}" SET {{ .Columns }} WHERE {{ .Conditions }}`
}

func (dameng *SDamengBackend) PrepareInsertOrUpdateSQL(ts sqlchemy.ITableSpec, insertColNames []string, insertFields []string, onPrimaryCols []string, updateSetCols []string, insertValues []interface{}, updateValues []interface{}) (string, []interface{}) {
	sqlTemp := `MERGE INTO "{{ .Table }}" T1 USING (SELECT {{ .SelectValues }} FROM DUAL) T2 ON ({{ .OnConditions }}) WHEN NOT MATCHED THEN INSERT({{ .Columns }}) VALUES ({{ .Values }}) WHEN MATCHED THEN UPDATE {{ .SetValues }}`
	selectValues := make([]string, 0, len(insertColNames))
	onConditions := make([]string, 0, len(onPrimaryCols))
	for _, colname := range insertColNames {
		selectValues = append(selectValues, fmt.Sprintf("? AS %s", colname))
	}
	for _, primary := range onPrimaryCols {
		onConditions = append(onConditions, fmt.Sprintf("T1.%s=T2.%s", primary, primary))
	}
	for i := range updateSetCols {
		setCol := updateSetCols[i]
		equalPos := strings.Index(setCol, "=")
		key := strings.TrimSpace(setCol[0:equalPos])
		val := strings.TrimSpace(setCol[equalPos+1:])
		key = strings.Trim(key, "`\"")
		key = fmt.Sprintf("T1.%s", key)
		updateSetCols[i] = fmt.Sprintf("%s = %s", key, val)
	}
	values := make([]interface{}, 0, len(insertValues)*2+len(updateValues))
	values = append(values, insertValues...)
	values = append(values, insertValues...)
	values = append(values, updateValues...)
	sql := sqlchemy.TemplateEval(sqlTemp, struct {
		Table        string
		SelectValues string
		OnConditions string
		Columns      string
		Values       string
		SetValues    string
	}{
		Table:        ts.Name(),
		SelectValues: strings.Join(selectValues, ", "),
		OnConditions: strings.Join(onConditions, " AND "),
		Columns:      strings.Join(insertColNames, ", "),
		Values:       strings.Join(insertFields, ", "),
		SetValues:    strings.Join(updateSetCols, ", "),
	})
	return sql, values
}

func (dameng *SDamengBackend) GetTableSQL() string {
	return `SELECT table_name AS "name" FROM user_tables`
}

func (dameng *SDamengBackend) CurrentUTCTimeStampString() string {
	return "GETUTCDATE()"
}

func (dameng *SDamengBackend) CurrentTimeStampString() string {
	return "GETDATE()"
}

func (dameng *SDamengBackend) GetCreateSQLs(ts sqlchemy.ITableSpec) []string {
	cols := make([]string, 0)
	primaries := make([]string, 0)
	for _, c := range ts.Columns() {
		cols = append(cols, c.DefinitionString())
		if c.IsPrimary() {
			primaries = append(primaries, fmt.Sprintf("`%s`", c.Name()))
		}
	}
	if len(primaries) > 0 {
		cols = append(cols, fmt.Sprintf("NOT CLUSTER PRIMARY KEY (%s)", strings.Join(primaries, ", ")))
	}
	sqls := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n%s\n)", ts.Name(), strings.Join(cols, ",\n")),
	}
	for _, idx := range ts.Indexes() {
		sqls = append(sqls, createIndexSQL(ts, idx))
	}
	return sqls
}

func (msyql *SDamengBackend) IsSupportIndexAndContraints() bool {
	return true
}

func (dameng *SDamengBackend) FetchTableColumnSpecs(ts sqlchemy.ITableSpec) ([]sqlchemy.IColumnSpec, error) {
	infos, err := fetchTableColInfo(ts)
	if err != nil {
		return nil, errors.Wrap(err, "fetchTableColInfo")
	}
	specs := make([]sqlchemy.IColumnSpec, 0)
	for _, info := range infos {
		specs = append(specs, info.toColumnSpec())
	}
	return specs, nil
}

func (dameng *SDamengBackend) FetchIndexesAndConstraints(ts sqlchemy.ITableSpec) ([]sqlchemy.STableIndex, []sqlchemy.STableConstraint, error) {
	indexes, err := fetchTableIndexes(ts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetchTableIndexes")
	}
	retIdxes := make([]sqlchemy.STableIndex, 0)
	for k := range indexes {
		if indexes[k].isPrimary {
			continue
		}
		retIdxes = append(retIdxes, sqlchemy.NewTableIndex(ts, indexes[k].indexName, indexes[k].colnames, false))
	}
	return retIdxes, nil, nil
}

func getTextSqlType(tagmap map[string]string) string {
	var width int
	var sqltype string
	widthStr := tagmap[sqlchemy.TAG_WIDTH]
	if len(widthStr) > 0 && regutils.MatchInteger(widthStr) {
		width, _ = strconv.Atoi(widthStr)
	}
	if width == 0 || width > 975 {
		sqltype = "TEXT"
	} else {
		sqltype = "VARCHAR"
	}
	return sqltype
}

func (dameng *SDamengBackend) GetColumnSpecByFieldType(table *sqlchemy.STableSpec, fieldType reflect.Type, fieldname string, tagmap map[string]string, isPointer bool) sqlchemy.IColumnSpec {
	switch fieldType {
	case tristate.TriStateType:
		col := NewTristateColumn(table.Name(), fieldname, tagmap, isPointer)
		return &col
	case gotypes.TimeType:
		col := NewDateTimeColumn(fieldname, tagmap, isPointer)
		return &col
	}
	switch fieldType.Kind() {
	case reflect.String:
		col := NewTextColumn(fieldname, getTextSqlType(tagmap), tagmap, isPointer)
		return &col
	case reflect.Int, reflect.Int32:
		col := NewIntegerColumn(fieldname, "INT", tagmap, isPointer)
		return &col
	case reflect.Int8:
		col := NewIntegerColumn(fieldname, "TINYINT", tagmap, isPointer)
		return &col
	case reflect.Int16:
		col := NewIntegerColumn(fieldname, "SMALLINT", tagmap, isPointer)
		return &col
	case reflect.Int64:
		col := NewIntegerColumn(fieldname, "BIGINT", tagmap, isPointer)
		return &col
	case reflect.Uint, reflect.Uint32:
		col := NewIntegerColumn(fieldname, "INT", tagmap, isPointer)
		return &col
	case reflect.Uint8:
		col := NewIntegerColumn(fieldname, "TINYINT", tagmap, isPointer)
		return &col
	case reflect.Uint16:
		col := NewIntegerColumn(fieldname, "SMALLINT", tagmap, isPointer)
		return &col
	case reflect.Uint64:
		col := NewIntegerColumn(fieldname, "BIGINT", tagmap, isPointer)
		return &col
	case reflect.Bool:
		col := NewBooleanColumn(fieldname, tagmap, isPointer)
		return &col
	case reflect.Float32, reflect.Float64:
		if _, ok := tagmap[sqlchemy.TAG_WIDTH]; ok {
			col := NewDecimalColumn(fieldname, tagmap, isPointer)
			return &col
		}
		colType := "REAL"
		if fieldType == gotypes.Float64Type {
			colType = "DOUBLE"
		}
		col := NewFloatColumn(fieldname, colType, tagmap, isPointer)
		return &col
	case reflect.Map, reflect.Slice:
		col := NewCompoundColumn(fieldname, getTextSqlType(tagmap), tagmap, isPointer)
		return &col
	}
	if fieldType.Implements(gotypes.ISerializableType) {
		col := NewCompoundColumn(fieldname, getTextSqlType(tagmap), tagmap, isPointer)
		return &col
	}
	return nil
}

func (dameng *SDamengBackend) QuoteChar() string {
	return "\""
}

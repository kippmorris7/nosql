//go:build nodynamodb
// +build nodynamodb

package dynamodb

import "github.com/smallstep/nosql/database"

type DB = database.NotSupportedDB

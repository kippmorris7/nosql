//go:build !nodynamodb
// +build !nodynamodb

package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/smallstep/nosql/database"
)

// DB encompasses the data needed for performing operations on
// DynamoDB tables
type DB struct {
	sdkConfig      aws.Config
	dynamoDbClient *dynamodb.Client
}

// Open sets the AWS SDK config and instantiates a dynamoDbClient configured to
// operate in the given AWS region.
func (db *DB) Open(awsRegion string, opt ...database.Option) (err error) {
	opts := &database.Options{}
	for _, o := range opt {
		if err := o(opts); err != nil {
			return err
		}
	}

	// TODO: How do I actually use context?
	ctx := context.TODO()

	// TODO: Do better than just loading the default config; maybe
	// check environment variables to choose an auth method?
	db.sdkConfig, err = config.LoadDefaultConfig(ctx)

	if err != nil {
		return err
	}

	db.sdkConfig.Region = awsRegion
	db.dynamoDbClient = dynamodb.NewFromConfig(db.sdkConfig)

	// Run a ListTables operation to validate the SDK config's
	// credentials and AWS region.
	_, err = db.dynamoDbClient.ListTables(ctx, &dynamodb.ListTablesInput{})

	if err != nil {
		return err
	}

	return nil
}

// Close does nothing; the DynamoDB client doesn't require any cleanup.
func (db *DB) Close() error {
	return nil
}

// CreateTable creates a region-level DynamoDB table.
func (db *DB) CreateTable(bucket []byte) error {
	tableName := string(bucket)

	// TODO: How do I actually use context?
	ctx := context.TODO()

	// TODO: Make things like deletion protection and provisioned throughput
	// configurable (and add other configuration options too)
	_, err := db.dynamoDbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:                 aws.String(tableName),
		DeletionProtectionEnabled: aws.Bool(false),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("nkey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("nvalue"),
				AttributeType: types.ScalarAttributeTypeB,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("nkey"),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(25),
			WriteCapacityUnits: aws.Int64(25),
		},
	})

	return err
}

// DeleteTable deletes a DynamoDB table...
func (db *DB) DeleteTable(bucket []byte) error {
	tableName := string(bucket)

	// TODO: How do I actually use context?
	ctx := context.TODO()

	_, err := db.dynamoDbClient.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	return err

	/*
		if err != nil {
			return err
		}

		// TODO: Figure out how to use a context to give this a timeout.
		for {
			_, err := db.dynamoDbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})

			if err != nil && errors.Is(err, types.TableNotFoundException) {
				break
			} else if err != nil {
				return err
			}
		}

		return nil
	*/
}

func (db *DB) Get(bucket, key []byte) (ret []byte, err error) {
	tableName := string(bucket)
	itemKey := string(key)

	// TODO: How do I actually use context?
	ctx := context.TODO()

	attributeValue, err := attributevalue.Marshal(itemKey)

	if err != nil {
		return nil, err
	}

	getItemOutput, err := db.dynamoDbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"nkey": attributeValue,
		},
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %s/%s", bucket, key)
	} else if len(getItemOutput.Item) == 0 {
		return nil, errors.Wrapf(database.ErrNotFound, "%s/%s not found", bucket, key)
	}

	err = attributevalue.Unmarshal(getItemOutput.Item["nvalue"], &ret)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (db *DB) Set(bucket, key, value []byte) error {
	tableName := string(bucket)
	itemKey := string(key)

	keyAttrValue, err := attributevalue.Marshal(itemKey)

	if err != nil {
		return err
	}

	valAttrValue, err := attributevalue.Marshal(value)

	if err != nil {
		return err
	}

	// TODO: How do I actually use context?
	ctx := context.TODO()

	_, err = db.dynamoDbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"nkey": keyAttrValue,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": valAttrValue,
		},
		UpdateExpression: aws.String("SET nvalue = :v"),
	})

	return err
}

func (db *DB) Del(bucket, key []byte) error {
	tableName := string(bucket)
	itemKey := string(key)

	// TODO: How do I actually use context?
	ctx := context.TODO()

	keyAttrValue, err := attributevalue.Marshal(itemKey)

	if err != nil {
		return err
	}

	_, err = db.dynamoDbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"nkey": keyAttrValue,
		},
	})

	return err
}

func (db *DB) List(bucket []byte) ([]*database.Entry, error) {
	tableName := string(bucket)

	// TODO: How do I actually use context?
	ctx := context.TODO()

	result := []*database.Entry{}

	var lastEvaluatedKey map[string]types.AttributeValue

	// Loop to paginate the results until resp.LastEvaluatedKey == nil
	for {
		resp, err := db.dynamoDbClient.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: lastEvaluatedKey,
		})

		if err != nil {
			return nil, err
		}

		for _, item := range resp.Items {
			var key, val []byte

			err = attributevalue.Unmarshal(item["nkey"], &key)

			if err != nil {
				return nil, err
			}

			err = attributevalue.Unmarshal(item["nvalue"], &val)

			if err != nil {
				return nil, err
			}

			dbEntry := database.Entry{
				Bucket: bucket,
				Key:    key,
				Value:  val,
			}

			result = append(result, &dbEntry)
		}

		lastEvaluatedKey = resp.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break
		}
	}

	return result, nil
}

func (db *DB) Update(tx *database.Tx) error {
	// TODO
	return nil
}

func (db *DB) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	// TODO
	return nil, false, nil
}

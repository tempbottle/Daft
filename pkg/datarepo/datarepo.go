package datarepo

import (
	"bytes"
	"context"
	"io"
	"path/filepath"

	"github.com/Eventual-Inc/Daft/pkg/datarepo/schema"
	"github.com/Eventual-Inc/Daft/pkg/objectstorage"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const SchemaFilename = "schema.yaml"

// Interface for reading from/writing to Datarepos
type StorageClient interface {
	// Writes a single Partfile, returning the ID of the file as a string
	WriteSchema(ctx context.Context, name string, version string, schema schema.Schema) error

	// Writes a single Partfile, returning the ID of the file as a string
	WritePartfile(ctx context.Context, name string, version string, fileData io.Reader) (string, error)
}

// AWS S3-based storage client
type S3StorageClient struct {
	S3bucket    string
	S3prefix    string
	ObjectStore objectstorage.ObjectStore
}

func (client *S3StorageClient) WriteSchema(ctx context.Context, name string, version string, schema schema.Schema) error {
	serializedSchema, err := yaml.Marshal(schema)
	if err != nil {
		return err
	}
	schemaPath := "s3://" + filepath.Join(client.S3bucket, client.S3prefix, name, version, SchemaFilename)
	client.ObjectStore.UploadObject(ctx, schemaPath, bytes.NewBuffer(serializedSchema))
	return nil
}

func (client *S3StorageClient) WritePartfile(ctx context.Context, name string, version string, fileData io.Reader) (string, error) {
	partId := uuid.New().String()
	path := "s3://" + filepath.Join(client.S3bucket, client.S3prefix, name, version, partId)
	client.ObjectStore.UploadObject(ctx, path, fileData)
	return partId, nil
}

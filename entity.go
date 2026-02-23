package uploader

import (
	"encoding/json"
	"log"
	"net/http"
)

const (
	Md5Sum    = "md5"
	Sha256Sum = "sha256"
)

type PresignedAction string

const (
	PresignedDownload PresignedAction = "GET"
	PresignedUpload   PresignedAction = "PUT"
)

type chunkRequest struct {
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
	FileId     string `json:"file_id"`
	ChunkNum   int    `json:"chunk_num"`
	Data       string `json:"data"`
	Checksum   string `json:"checksum"`
}

type deleteFileRequest struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
}

type downloadFileRequest struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
}

type getPresignedObjectRequest struct {
	Action       PresignedAction `json:"action"`
	AllowedTypes []string        `json:"allowed_types"`
	Bucket       string          `json:"bucket"`
	ExpiresIn    int             `json:"expires_in"`
	Path         string          `json:"path"`
}

type listObjectsRequest struct {
	Bucket string `json:"bucket" binding:"required"`
	Path   string `json:"path"`
}

type ObjectInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
}

type CreatePresignedUrlOptions struct {
	AllowedTypes []string
	ExpiresIn    int
}

type serverErrorBody struct {
	ErrorCode string   `json:"code"`
	Message   string   `json:"message"`
	Details   []string `json:"details"`
}

type serverResponse struct {
	Data    json.RawMessage  `json:"data"`
	Message string           `json:"message"`
	Error   *serverErrorBody `json:"error"`
}

type statObjectRequest struct {
	Bucket string `json:"bucket" binding:"required"`
	Path   string `json:"path" binding:"required"`
}

type UploadClientOptions struct {
	AccessKey      string
	ChunkSize      int
	Endpoint       string
	HTTPClient     *http.Client
	Logger         *log.Logger
	MaxConcurrency int
}

type UploadOptions struct {
	OnProgress        func(uploaded, total int64)
	ChecksumAlgorithm string
}

type UploadResult struct {
	Checksum string
	Size     int64
	Path     string
}

type verifyRequest struct {
	Bucket   string `json:"bucket"`
	Path     string `json:"path"`
	FileId   string `json:"file_id"`
	Checksum string `json:"checksum"`
}

type chunkJob struct {
	data     []byte
	chunkNum int
}

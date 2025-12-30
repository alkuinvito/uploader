package uploader

import "net/http"

const (
	Md5Sum    = "md5"
	Sha256Sum = "sha256"
)

type UploadClientOptions struct {
	AccessKey  string
	ChunkSize  int
	Endpoint   string
	HTTPClient *http.Client
}

type UploadOptions struct {
	OnProgress        func(uploaded, total int64)
	ChecksumAlgorithm string
}

type UploadResult struct {
	Checksum string
	Size     int64
}

type ChunkRequest struct {
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
	FileId     string `json:"file_id"`
	ChunkNum   int    `json:"chunk_num"`
	Data       string `json:"data"`
	Checksum   string `json:"checksum"`
}

type VerifyRequest struct {
	Bucket   string `json:"bucket"`
	Path     string `json:"path"`
	FileId   string `json:"file_id"`
	Checksum string `json:"checksum"`
}

type GetObjectRequest struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
}

type DeleteFileRequest struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
}

type DownloadFileRequest struct {
	Bucket string `json:"bucket"`
	Path   string `json:"path"`
}

type ListObjectsRequest struct {
	Bucket string `json:"bucket" binding:"required"`
	Path   string `json:"path"`
}

type StatObjectRequest struct {
	Bucket string `json:"bucket" binding:"required"`
	Path   string `json:"path" binding:"required"`
}

type ObjectInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
}

package uploader

import "net/http"

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

const (
	Md5Sum    = "md5"
	Sha256Sum = "sha256"
)

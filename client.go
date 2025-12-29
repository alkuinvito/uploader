package uploader

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
)

type UploadClient struct {
	accessKey  string
	chunkSize  int
	endpoint   string
	httpClient *http.Client
}

type IUploadClient interface {
	calculateChecksum(data []byte, algorithm string) (string, error)
	PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error)
	sendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error
	verifyChecksum(bucketName, objectName, fileId, expectedChecksum string) error
}

// Request structs matching server's exact JSON tags
type chunkRequest struct {
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
	FileId     string `json:"file_id"`
	ChunkNum   int    `json:"chunk_num"`
	Data       string `json:"data"`
	Checksum   string `json:"checksum"`
}

type verifyRequest struct {
	Bucket   string `json:"bucket"`
	Path     string `json:"path"`
	FileId   string `json:"file_id"`
	Checksum string `json:"checksum"`
}

// NewClient creates a new upload client
func NewClient(options *UploadClientOptions) *UploadClient {
	return &UploadClient{
		accessKey:  options.AccessKey,
		chunkSize:  options.ChunkSize,
		endpoint:   options.Endpoint,
		httpClient: options.HTTPClient,
	}
}

// NewClientWithDefaults creates a new upload client with default options
func NewClientWithDefaults(endpoint, accessKey string) *UploadClient {
	return NewClient(&UploadClientOptions{
		AccessKey:  accessKey,
		ChunkSize:  1024 * 1024, // 1MB default
		Endpoint:   endpoint,
		HTTPClient: &http.Client{},
	})
}

func (c *UploadClient) calculateChecksum(data []byte, algorithm string) (string, error) {
	switch algorithm {
	case Sha256Sum, "":
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:]), nil
	case Md5Sum:
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:]), nil
	default:
		return "", fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

// PutObject uploads a base64-encoded file in chunks with checksum verification
func (c *UploadClient) PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error) {
	totalSize := int64(len(data))
	reader := bytes.NewReader(data)

	// Determine checksum algorithm
	checksumAlgo := Sha256Sum
	if opts != nil && opts.ChecksumAlgorithm != "" {
		checksumAlgo = opts.ChecksumAlgorithm
	}

	// Calculate checksum
	checksum, err := c.calculateChecksum(data, checksumAlgo)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Generate unique file ID for this upload
	fileId := uuid.New().String()

	// Upload in chunks
	var uploaded int64
	chunkNum := 0

	for {
		chunk := make([]byte, c.chunkSize)
		n, err := reader.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}

		if n == 0 {
			break
		}

		// Send chunk to server
		if err := c.sendChunk(bucketName, objectName, fileId, chunk[:n], chunkNum, checksum); err != nil {
			return nil, fmt.Errorf("failed to send chunk %d: %w", chunkNum, err)
		}

		uploaded += int64(n)
		chunkNum++

		// Progress callback
		if opts != nil && opts.OnProgress != nil {
			opts.OnProgress(uploaded, totalSize)
		}

		if err == io.EOF {
			break
		}
	}

	// Verify with server
	if err := c.verifyChecksum(bucketName, objectName, fileId, checksum); err != nil {
		return nil, fmt.Errorf("checksum verification failed: %w", err)
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

func (c *UploadClient) sendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/upload/chunk", c.endpoint)

	// Encode chunk to base64 for transmission
	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	// Create request with bucket_name and object_name tags
	payload := chunkRequest{
		BucketName: bucketName,
		ObjectName: objectName,
		FileId:     fileId,
		ChunkNum:   chunkNum,
		Data:       encodedChunk,
		Checksum:   checksum,
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *UploadClient) verifyChecksum(bucketName, objectName, fileId, expectedChecksum string) error {
	url := fmt.Sprintf("%s/upload/verify", c.endpoint)

	// Create verify request with bucket and path tags
	payload := verifyRequest{
		Bucket:   bucketName,
		Path:     objectName,
		FileId:   fileId,
		Checksum: expectedChecksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal verify request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("verification failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

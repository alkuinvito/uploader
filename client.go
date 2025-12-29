package uploader

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
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
	sendChunk(chunk []byte, filename string, chunkNum int, checksum string) error
	verifyChecksum(filename, expectedChecksum string) error
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
func NewClientWithDefaults(endpoint string) *UploadClient {
	return NewClient(&UploadClientOptions{
		Endpoint:   endpoint,
		ChunkSize:  1024 * 1024, // 1MB default
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

	// Calculate checksum
	checksum, err := c.calculateChecksum(data, opts.ChecksumAlgorithm)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

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
		if err := c.sendChunk(chunk[:n], bucketName, objectName, chunkNum, checksum); err != nil {
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
	if err := c.verifyChecksum(bucketName, objectName, checksum); err != nil {
		return nil, fmt.Errorf("checksum verification failed: %w", err)
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

func (c *UploadClient) sendChunk(chunk []byte, bucketName, objectName string, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/uploads/chunk", c.endpoint)

	// Encode chunk back to base64 for transmission
	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	payload := fmt.Sprintf(`{"bucket_name":"%s","object_name":"%s","chunk_num":%d,"data":"%s","checksum":"%s"}`,
		bucketName, objectName, chunkNum, encodedChunk, checksum)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
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
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

func (c *UploadClient) verifyChecksum(bucketName, objectName, expectedChecksum string) error {
	url := fmt.Sprintf("%s/uploads/verify?bucket=%s&object_name=%s&checksum=%s",
		c.endpoint, bucketName, objectName, expectedChecksum)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-API-KEY", c.accessKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verification failed with status %d", resp.StatusCode)
	}

	return nil
}

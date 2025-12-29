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
	ServerURL  string
	ChunkSize  int
	HTTPClient *http.Client
}

type UploadClientOptions struct {
	ServerURL  string
	ChunkSize  int
	HTTPClient *http.Client
}

type UploadOptions struct {
	OnProgress        func(uploaded, total int64)
	ChecksumAlgorithm string // "md5" or "sha256"
}

type UploadResult struct {
	Checksum string
	Size     int64
}

// NewClient creates a new upload client
func NewClient(options *UploadClientOptions) *UploadClient {
	return &UploadClient{
		ServerURL:  options.ServerURL,
		ChunkSize:  options.ChunkSize,
		HTTPClient: options.HTTPClient,
	}
}

// NewClientWithDefaults creates a new upload client with default options
func NewClientWithDefaults(serverURL string) *UploadClient {
	return NewClient(&UploadClientOptions{
		ServerURL:  serverURL,
		ChunkSize:  1024 * 1024, // 1MB default
		HTTPClient: &http.Client{},
	})
}

// UploadBase64 uploads a base64-encoded file in chunks with checksum verification
func (c *UploadClient) UploadBase64(base64String string, filename string, opts *UploadOptions) (*UploadResult, error) {
	// Decode base64 string
	decoded, err := base64.StdEncoding.DecodeString(base64String)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	totalSize := int64(len(decoded))
	reader := bytes.NewReader(decoded)

	// Calculate checksum
	checksum, err := c.calculateChecksum(decoded, opts.ChecksumAlgorithm)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Upload in chunks
	var uploaded int64
	chunkNum := 0

	for {
		chunk := make([]byte, c.ChunkSize)
		n, err := reader.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}

		if n == 0 {
			break
		}

		// Send chunk to server
		if err := c.sendChunk(chunk[:n], filename, chunkNum, checksum); err != nil {
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
	if err := c.verifyChecksum(filename, checksum); err != nil {
		return nil, fmt.Errorf("checksum verification failed: %w", err)
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

func (c *UploadClient) calculateChecksum(data []byte, algorithm string) (string, error) {
	switch algorithm {
	case "sha256", "":
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:]), nil
	case "md5":
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:]), nil
	default:
		return "", fmt.Errorf("unsupported algorithm: %s", algorithm)
	}
}

func (c *UploadClient) sendChunk(chunk []byte, filename string, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/uploads/chunk", c.ServerURL)

	// Encode chunk back to base64 for transmission
	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	payload := fmt.Sprintf(`{"filename":"%s","chunk_num":%d,"data":"%s","checksum":"%s"}`,
		filename, chunkNum, encodedChunk, checksum)

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	return nil
}

func (c *UploadClient) verifyChecksum(filename, expectedChecksum string) error {
	url := fmt.Sprintf("%s/uploads/verify?filename=%s&checksum=%s",
		c.ServerURL, filename, expectedChecksum)

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verification failed with status %d", resp.StatusCode)
	}

	return nil
}
